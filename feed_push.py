#!/usr/bin/env python 

# Feed pusher: Gets an ALE feed and pushes it to a DB.
# Only currently supported DB is MongoDB.
# It can also be used to dump protobuf messages to JSON.
#
# Needs python 2.7 (at least) and pymongo to push to MongoDB.
# For JSON dumping, can work with Python 2.6

from __future__ import print_function

import fileinput
import json
import signal
import sys
import functools
import ssl

from optparse import OptionParser # We must support python 2.6...
from datetime import datetime
from multiprocessing import Process, Pool, Queue
from collections import defaultdict
from Queue import Empty, Full


# Maximum number of transforming processes
MAX_PROCS     = 100
DEFAULT_PROCS = 5
# Size of the chunks to be sent to each worker thread
MAX_CHUNK     = 100
DEFAULT_CHUNK = 10

# Global initializer that runs in the context of a worker pool process.
# Runs the factory to build an action local to this process.
def _initialize():
    try:
        global _action, _factory, _failed
        _action = _factory()
        _failed = False
    except:
        # TODO: Handle failures more gracefully...
        _failed = True
_factory = None
_failed  = False

# Global action that runs per event
# Use the action built by _factory() above to process each event.
def _run(event):
    global _action, _failed
    # If init failed, all events are skipped...
    if _failed:
        return "Factory failed, skipping events"
    return _action(event)
_action  = None


# Generates a stream of events from input text.
# Lines must be in the format generated by feed-reader.
def events_from_lines(lines):
    current = dict() # Current object
    nesting = list() # Nesting list: Open items
    for line in lines:
        # Check for new event start
        line = line.strip()
        if "Recv event with topic" in line:
            if len(nesting) > 0:
                # We did not detect the end of the previous topic...
                # discard it
                nesting = list()
                current = dict()
            # Otherwise, if we have a well-formed event, return it
            elif len(current) > 0:
                yield current
                current = dict()
            # And start a new topic
            topic = line.split()[-1]
            # Remove quotes from topic name
            current["topic"] = topic[1:-1]
        # Check for nesting
        elif line.endswith("{"):
            nesting.append((current, line.split()[0]))
            current = dict()
        # Check for un-nesting
        elif line.endswith("}") and len(nesting) > 0:
            top, nested = nesting.pop()
            # If we are not at the top, save the current item as a
            # property of the upper (top) item.
            if len(nesting) > 0:
                prev = top.setdefault(nested, current)
                if prev != current:
                    # Several instances of this field!
                    try:
                        # If the item is already a list, push the new value
                        prev.append(current)
                    except:
                        # If it is not a list already, make a new one
                        prev = [prev, current]
                    # And restore the list
                    top[nested] = prev
            # If we are back at the top, as a special case, we want to
            # flatten the item.
            else:
                for k, v in current.iteritems():
                    top[k] = v
            # And work with the "top" object from now on.
            current = top
        # Check for any other parameter
        elif line != "" and not line.isspace():
            parts = line.split(":", 1)
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip()
                # length check avoids trying to convert MACs, hashes to ints.
                # it is just accurate enough for our purposes.
                if val and (len(val) < 12):
                    # This should catch both positive counters (int)
                    # and locations (float). If ALE yields negative numbers
                    # for any of these, let me know...
                    if val.replace(".", "", 1).isdigit():
                        try:
                            newVal = float(val)
                        except ValueError:
                            pass
                        else:
                            val = newVal
                current[key] = val
    # No more lines. If we have a current object, yield it.
    if len(current) > 0:
        yield current

# Distributes the events across several processes:
# - Creates a new Pool with "numprocs" processes
# - Initializes each process calling factory().
#   factory() should return an action (a callable).
# - Distributes events amongst the processes.
# - Each process calls action(event), for every event
#   ('action' being the callable returned by 'factory')
#
# Params:
# - numprocs: number of processes in the pool
# - chunksize: per-process buffer.
# - rate: if True, discards events when all buffers are full.
#   If False, blocks until some buffer is free.
def parallel(events, factory, numprocs, chunksize, rate=False):
    # If rate limiting enabled, wrap the events and the actions
    # in a rate-limiting producer and consumer
    if rate:
        # "chunksize" tokens per proc, plus another chunksize for burst
        tokens  = chunksize * (numprocs+1)
        # Share the tokens amongst the processes
        perproc = (tokens/numprocs) + 1
        # Allocate a queue with enough capacity for all perproc tokens
        queue   = Queue(numprocs * perproc)
        # And tie the consumer and producer to the queue
        factory = _consumer(queue, perproc, factory)
        events  = _producer(queue, events)
    # Set the factory
    global _factory
    _factory = factory
    # Build a pool. Make child processes ignore SIGINT.
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = Pool(processes=numprocs, initializer=_initialize)
    signal.signal(signal.SIGINT, original_sigint_handler)
    # Loop through the events!
    try:
        for result in pool.imap_unordered(_run, events, chunksize=chunksize):
            if result is not None:
                yield result
        pool.close()
    except KeyboardInterrupt:
        pool.terminate()
    except:
        pool.terminate()
        raise # Propagate the unexpected error, for troubleshooting
    finally:
        # Cleanup processes
        pool.join()

# Sample factory that returns each input event as json.
def json_factory(pprint):
    def action(event):
        ts = event.get("timestamp", None)
        if ts is not None:
            # Foirmat timestamp as a proper date
            ts = datetime.fromtimestamp(ts)
            event["timestamp"] = ts.strftime('%Y-%m-%d %H:%M:%S')
        if pprint:
            return json.dumps(event, indent=4, sort_keys=True)
        return json.dumps(event)
    return action

# Sample factory that stores events in mongodb.
# Adds two properties to each event:
# - "opcode": 0 for UP_UPDATE, 1 for OP_ADD, -1 for OP_DELETE.
# - "datetime": timestamp of the event, in datetime format
def mongo_factory(mongouri, cap=0):
    import pymongo # Delayed import - only if this factory is used.
    client = pymongo.MongoClient(mongouri,
        # Redhat (6.x) + python (2.6) + mongo (3.5) is a difficult
        # combo for proper TLS... since this is a PoC, just ignore it.
        ssl_cert_reqs=ssl.CERT_NONE)
    db = client.ale
    # Ping the DB to fail early if it is not reachable
    db.command('ping')
    # This action does not expect a single event, but a list of events!
    # This way we can be more efficient when sending to mongodb.
    def action(events):
        topics = defaultdict(list)
        result = list()
        capped = dict()
        for event in events:
            # Mandatory fields
            topic = event.get("topic", None)
            ts    = event.get("timestamp", None)
            op    = event.get("op", None)
            # Some messages, like visibility_records, do not have an opcode.
            # But all of them have topic and timestamp.
            if topic is None or ts is None:
                result.append("invalid: %s" % json.dumps(event))
                continue
            # We will have a collection per topic.
            # Topics like "location" have the format "location/<mac>".
            # Let's remove the MAC in the collection's name.
            topic = topic.split("/", 1)[0]
            # Store op code as an integer
            # (OP_ADD = 1, OP_UPDATE = 0, OP_DELETE = -1)
            if op is not None:
                if   op == "OP_UPDATE":
                    opcode = 0
                elif op == "OP_ADD":
                    opcode = 1
                elif op == "OP_DELETE":
                    opcode = -1
                else:
                    return None
                event["opcode"] = opcode
            # Also store timestamp as a datetime
            event["datetime"] = datetime.fromtimestamp(ts)
            topics[topic].append(event)
        # Insert documents in the corresponding topic collection
        for topic, docs in topics.iteritems():
            if cap > 0 and capped.get(topic, None) is None:
                # If we got a cap and had not seen the topic before,
                # then try to create a capped collection
                try:
                    db.create_collection(topic, capped=True, size=cap)
                except pymongo.errors.CollectionInvalid:
                    pass
                except pymongo.errors.OperationFailure:
                    pass
                finally:
                    capped[topic] = True
            result.append("%s: %d" % (topic, len(docs)))
            db[topic].insert_many(docs)
        return ", ".join(result)
    return action


# Makes sure a value is within bounds
def _bounded(original, default, maximum):
    # If "original" comes from command line, it may be a string
    try:
        value = int(original)
    except ValueError:
        value = 0
    # Check bounds
    if value < default:
        value = default
    if value > maximum:
        value = maximum
    return value

# Rate-limited producer 'decorator'. Uses the queue as sort of a token-bucket.
def _producer(queue, events):
    discarding = False
    for event in events:
        try:
            token = queue.get_nowait()
            yield event
            discarding = False
        except Empty:
            if not discarding:
                print("Event rate is too high or numprocs too low, "+
                      "discarding events...", file=sys.stderr)
            discarding = True
            pass

# Rate-limited factory 'decorator'. Refills the queue on each event processed.
def _consumer(queue, tokens, factory):
    def inner_factory():
        # Get the actual factory action
        action = factory()
        # Actual action will return tokens back to the queue
        def inner_action(event):
            try:
                return action(event)
            finally:
                queue.put(True)
        # Action ready, insert first tokens in the queue.
        for i in xrange(0, tokens):
            queue.put_nowait(True)
        # And return the action.
        return inner_action
    return inner_factory

# Generates event lists with several events each
def _chunk(events, chunksize):
    current = list()
    for event in events:
        if len(current) >= chunksize:
            yield current
            current = list()
        current.append(event)
    if len(current) > 0:
        yield current


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-n", "--numprocs", action="store", dest="numprocs",
        default=DEFAULT_PROCS, metavar="NUMBER_OF_PROCS",
        help="Number of transform processes to run")
    parser.add_option("-c", "--capsize", action="store", dest="capsize",
        default=0, metavar="CAP SIZE (MBytes)",
        help="Cap mongodb collections to the given size")
    parser.add_option("-s", "--chunksize", action="store", dest="chunksize",
        default=DEFAULT_CHUNK, metavar="CHUNK_SIZE",
        help="Size of chunk for each process")
    parser.add_option("-r", action="store_true", dest="rate",
        default=False,
        help="Rate-limit input - useful for inline processing")
    parser.add_option("-m", action="store", dest="mongouri",
        default="",
        help="MongoDB URI connection string (e.g. -m \"$MONGODB_URI\")")
    parser.add_option("-p", action="store_true", dest="pprint",
        default=False,
        help="Pretty-print results (add newlines, indentation, etc)")
    parser.add_option("-q", action="store_true", dest="quiet",
        default=False,
        help="Be quiet - do not print anything to stdout")
    options, args = parser.parse_args()

    # Get the number of procs from options parameters
    numprocs  = _bounded(options.numprocs, DEFAULT_PROCS, MAX_PROCS)
    chunksize = _bounded(options.chunksize, DEFAULT_CHUNK, MAX_CHUNK)
    mongouri = options.mongouri

    events = events_from_lines(fileinput.input(args))
    # Get the factory to use, MongoDB or JSON
    if mongouri == "":
        factory = functools.partial(json_factory, options.pprint)
    else:
        capsize = int(options.capsize) << 20 # MBytes to bytes
        factory = functools.partial(mongo_factory, mongouri, capsize)
        # The mongo factory does not expect individual events, but lists
        # of events. We have to wrap the event iterator, and then use
        # chunksize = 1 for the pool, so we do not chunk twice.
        events    = _chunk(events, chunksize)
        chunksize = 1
    # And run the pool
    quiet = options.quiet
    for result in parallel(events, factory, numprocs, chunksize, options.rate):
        if not quiet:
            print(result)

