#!/usr/bin/env python 

# Feed pusher: Gets an ALE feed and pushes it to a DB.
# Only currently supported DB is MongoDB.
# It can also be used to dump protobuf messages to JSON or text.
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
import time

from random import randint
from optparse import OptionParser # We must support python 2.6...
from datetime import datetime
from multiprocessing import Pool, Queue
from Queue import Empty

# Maximum number of transforming processes
MAX_PROCS = 100
DEFAULT_PROCS = 1
# Size of the chunk of events sent to each process
MAX_CHUNK = 100
DEFAULT_CHUNK = 5
# Number of chunks that can be buffered per process
MAX_RATE = 100
DEFAULT_RATE = 1
# Default backoff time (secs), and max number of times to back off
MAX_RETRIES = 10
DEFAULT_BACKOFF = 5

# State for each process of the Pool
POOLDATA = None

class PoolData(object):
    """State for each of the processes of the pool"""

    def __init__(self, factory, backoff=DEFAULT_BACKOFF):
        """Sets the initial state using the provided factory"""
        self._factory = factory
        self._action = None
        self._backoff = backoff
        # Try to initialize
        self.action()

    def action(self, block=False):
        """Returns the action to run, or None on failure"""
        if self._action is not None:
            return self._action
        if not block:
            return None
        numtry = 0
        while self._action is None:
            try:
                self._action = self._factory()
            except Exception as e:
                print("Exception while initializing factory: " + str(e), file=sys.stderr)
                # Just to make sure
                self._action = None
                self.backoff(numtry)
                numtry += 1
        return self._action

    def dispatch(self, event):
        """Dispatches event, retries until success"""
        numtry = 0
        while True:
            action = self.action(block=True)
            try:
                return action(event)
            except Exception as e:
                print("Failed to process event: " + str(e), file=sys.stderr)
                self._action = None
                self.backoff(numtry)
                numtry += 1

    def backoff(self, numtry):
        """Sleeps for a bit after a failure"""
        # Linearly increment backoff with each retry, until MAX_BACKOFF
        if numtry > MAX_RETRIES:
            numtry = MAX_RETRIES
        backoff = self._backoff * (numtry+1)
        # Randomize backoff to avoid process synchronization
        backoff = randint(int(backoff*0.5)+1, int(backoff*1.5))
        print("Entering backoff for %d seconds" % backoff, file=sys.stderr)
        time.sleep(backoff)
        print("Backoff finished", file=sys.stderr)

def _initialize(factory, backoff):
    """Global initializer that runs in each worker pool process.
    Runs the factory to build an action local to this process"""
    global POOLDATA
    POOLDATA = PoolData(factory, backoff)

def _run(event):
    """Global action that runs per event.
    Use the action built by factory() to process each event"""
    return POOLDATA.dispatch(event)

class DictObject(dict):
    """Dict-like objects which keys can be accesed as properties"""
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

def _trynumber(value):
    """Try to convert a string to a number"""
    # Let's catch true / false here too
    if value in ["true", "false"]:
        return True if value == "true" else False
    # This should catch both positive counters (int)
    # and locations (float). If ALE yields negative numbers
    # for any of these, let me know...
    clean = value.replace(".", "", 1)
    if clean.isdigit():
        try:
             # If there was no ".", try with int
            if len(value) == len(clean):
                return int(value)
            # Otherwise, try with float
            else:
                return float(value)
        except ValueError:
            pass
    return value

def _trymac(value):
    """Try to convert a string to a MAC address"""
    # Remove separator characters
    # This is the python 2.X way to do it...
    # TODO: adapt for python 3.
    mac = value.translate(None, ".:-").lower()
    if mac.isalnum() and len(mac) == 12:
        # colon-separated, lowercase format
        return ':'.join(mac[i:i+2] for i in range(0, 12, 2))
    return value

def _nest(top, key, nested, embed=False):
    """Embeds or inserts a DictObject into another one"""
    # Subgroups with just one item are inlined.
    if len(nested) == 1:
        value = nested.values()[0]
        # Many of these nested groups are MAC addresses
        if key.endswith("_mac"):
            value = _trymac(value)
        # Save the attribute at top. Ignore the key.
        top[key] = value
    # Larger subgroups may be embedded
    elif embed:
        for key, val in nested.iteritems():
            top[key] = val
    else:
        prev = top.setdefault(key, nested)
        if prev != nested:
            # Several instances of this field!
            appendable = getattr(prev, "append", None)
            if appendable is not None:
                # If the item is already a list, push the new value
                prev.append(nested)
            else:
                # If it is not a list already, make a new one
                prev = [prev, nested]
            # And restore the list
            top[key] = prev
    return top

def events_from_lines(lines):
    """Generates a stream of events from input text.
    Lines must be in the format generated by feed-reader"""
    current = DictObject() # Current object
    nesting = list() # Nesting list: Open items
    for line in lines:
        # Check for new event start
        line = line.strip()
        if "Recv event with topic" in line:
            # Check the event is well-formed before yielding it
            if len(nesting) <= 0 and len(current) > 0:
                yield current
            # And start a new event
            current = DictObject()
            nesting = list()
            # Remove quotes from topic name
            topic = line.split()[-1]
            current["topic"] = topic[1:-1]
        # Check for nesting
        elif line.endswith("{"):
            nesting.append((current, line.split()[0]))
            current = DictObject()
        # Check for un-nesting
        elif line.endswith("}") and len(nesting) > 0:
            top, key = nesting.pop()
            # Fields of the top groups are embedded, others are nested
            current = _nest(top, key, current, embed=(len(nesting)<=0))
        # Check for any other parameter
        elif line and not line.isspace():
            parts = line.split(":", 1)
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip()
                # length check avoids trying to convert MACs, hashes to ints.
                # it is just accurate enough for our purposes.
                if val and (len(val) < 12):
                    val = _trynumber(val)
                current[key] = val
    # No more lines. If we have a current object, yield it.
    if len(current) > 0:
        yield current

class Limiter(object):
    """Rate-limited factory 'decorator'. Refills the queue on each event processed"""

    def __init__(self, numprocs, rate, factory):
        # "rate" tokens per proc, plus another "rate" for burst
        tokens = rate * (numprocs+1)
        # Share the tokens amongst the processes
        perproc = (tokens/numprocs)
        self._queue = Queue(numprocs * (perproc + 1))
        # Beware! the pool won't call "initialize" until there are events
        # to process, and there won't be even to process until the queue
        # has items... so we have to add some tokens to que queue, to
        # bootstrap this.
        for _ in xrange(0, numprocs):
            self._queue.put(True)
        self._factory = factory
        self._tokens = perproc

    def events(self, events):
        """Decorate an event stream to rate-limit it"""
        discarding = False
        for event in events:
            try:
                _ = self._queue.get_nowait()
                yield event
                if discarding:
                    print("Event rate lowered, processing events again...", file=sys.stderr)
                discarding = False
            except Empty:
                if not discarding:
                    print("Event rate is too high or numprocs too low, "+
                        "discarding events...", file=sys.stderr)
                discarding = True

    def __call__(self):
        """Generates action calling the decorated factory"""
        # Insert some tokens in the queue for this process
        if self._tokens > 0:
            for _ in xrange(0, self._tokens):
                self._queue.put(True)
            self._tokens = 0
        # Get the actual factory action
        action = self._factory()
        if action is None:
            return None
        # Actual action will return tokens back to the queue
        def _inner_action(event):
            try:
                return action(event)
            finally:
                self._queue.put(True)
        # And return the action.
        return _inner_action

def _chunk(events, chunksize):
    """Generates event lists with several events each"""
    current = list()
    for event in events:
        if len(current) >= chunksize:
            yield current
            current = list()
        current.append(event)
    if len(current) > 0:
        yield current

def parallel(events, factory, numprocs, chunksize, backoff):
    """Distributes the events across several processes.

    - Creates a new Pool with "numprocs" processes
    - Initializes each process calling factory().
      factory() should return an action (a callable).
    - Distributes events amongst the processes.
    - Each process calls action(event), for every event
      ('action' being the callable returned by 'factory')

    Params:
    - numprocs: number of processes in the pool
    - chunksize: per-process event buffer.
    - backoff: seconds between retries
    """
    # Build a pool. Make child processes ignore SIGINT.
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = Pool(processes=numprocs, initializer=_initialize, initargs=(factory, backoff))
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

def json_factory(pprint):
    """Simple factory for returning each input event as json"""
    def _action(event):
        tstamp = event.get("timestamp", None)
        if tstamp is not None:
            # Format timestamp as a proper date
            tstamp = datetime.fromtimestamp(tstamp)
            event["timestamp"] = tstamp.strftime('%Y-%m-%d %H:%M:%S')
        if pprint:
            return json.dumps(event, indent=4, sort_keys=True)
        return json.dumps(event)
    return _action

def text_factory(topic_formats, default_format=None):
    """Simple factory for returning each input event as text"""
    def _action(event):
        topic = event.get("topic", None)
        if topic is None:
            return None
        tstamp = event.get("timestamp", None)
        if tstamp is not None:
            # Format timestamp as a proper date
            tstamp = datetime.fromtimestamp(tstamp)
            event["timestamp"] = tstamp.strftime('%Y-%m-%d %H:%M:%S')
        # Require only a partial match
        try:
            for key, topic_format in topic_formats.iteritems():
                if topic.startswith(key):
                    return topic_format.format(**event)
            if default_format is not None:
                return default_format.format(**event)
            return None
        except KeyError as err:
            return "KEYERROR: {0} NOT FOUND IN {1!r}".format(err, event)
    return _action

class _MongoDB(object):
    """MongoDB manager"""

    def __init__(self, mongouri, cap):
        import pymongo # Delayed import - only if this factory is used.
        self.pymongo = pymongo
        self.cap     = cap
        self.capped  = dict()
        client = pymongo.MongoClient(mongouri,
            # Redhat (6.x) + python (2.6) + mongo (3.5) is a difficult
            # combo for proper TLS... since this is a PoC, just ignore it.
            ssl_cert_reqs=ssl.CERT_NONE)
        self.mongodb = client.ale
        # Ping the DB to fail early if it is not reachable
        self.mongodb.command('ping')

    def insert(self, topic, docs):
        """Insert documents in the corresponding mongodb collection"""
        if (self.cap > 0) and (self.capped.get(topic, None) is None):
            success = False
            try:
                self.mongodb.create_collection(topic, capped=True, size=self.cap)
                success = True
            except self.pymongo.errors.CollectionInvalid:
                # Collection will never be created
                success = True
            except self.pymongo.errors.OperationFailure:
                # Collection exists already
                success = True
            finally:
                # Other exceptions can be retried
                self.capped[topic] = success
        self.mongodb[topic].insert_many(docs)

def mongo_factory(mongouri, cap=0):
    """Factory to store events in mongodb.
    Adds two properties to each event:
    - "opcode": 0 for UP_UPDATE, 1 for OP_ADD, -1 for OP_DELETE.
    - "datetime": timestamp of the event, in datetime format
    """
    mongodb = _MongoDB(mongouri, cap)
    # This action does not expect a single event, but a list of events!
    # This way we can be more efficient when sending to mongodb.
    def _action(events):
        # Sort event list in place. If we fail while inserting items
        # into the collection, the request will be backed off and
        # retried. To avoid duplicate inserts, we want to remove individual
        # events as we process them.
        events.sort(key=lambda event: event.get("topic", None))
        result = list()
        prevtopic = None
        for cursor in xrange(len(events), 0, -1):
            event = events[cursor-1]
            # Mandatory fields
            topic = event.get("topic", None)
            tstamp = event.get("timestamp", None)
            oper = event.get("op", None)
            # Some messages, like visibility_records, do not have an opcode.
            # But all of them have topic and timestamp.
            if topic is None or tstamp is None:
                result.append("invalid: %s" % json.dumps(event))
                continue
            # We will have a collection per topic.
            # Topics like "location" have the format "location/<mac>".
            # Let's remove the MAC in the collection's name.
            topic = topic.split("/", 1)[0]
            # Store op code as an integer
            # (OP_ADD = 1, OP_UPDATE = 0, OP_DELETE = -1)
            if oper is not None:
                if oper == "OP_UPDATE":
                    event["opcode"] = 0
                elif oper == "OP_ADD":
                    event["opcode"] = 1
                elif oper == "OP_DELETE":
                    event["opcode"] = -1
            # Also store timestamp as a datetime
            event["datetime"] = datetime.fromtimestamp(tstamp)
            # If we change topic, store events
            if topic != prevtopic:
                if cursor < len(events):
                    docs = events[cursor:]
                    result.append("%s: %d" % (prevtopic, len(docs)))
                    mongodb.insert(prevtopic, docs)
                    # If no exception triggered, remove the
                    # docs from the event list, trimming in place
                    del events[cursor:]
                prevtopic = topic
        # Save the remaining events
        if (len(events) > 0) and (prevtopic is not None):
            result.append("%s: %d" % (prevtopic, len(events)))
            mongodb.insert(prevtopic, events)
        return ", ".join(result)
    return _action

def _bounded(original, default, maximum):
    """Makes sure a value is within bounds"""
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

def _cmd():
    """Command line processing"""

    parser = OptionParser()
    parser.add_option(
        "-b", "--backoff", action="store", dest="backoff",
        default=DEFAULT_BACKOFF, metavar="backoff time (seconds)",
        help="Seconds to wait between retries, on failure")
    parser.add_option(
        "-c", "--capsize", action="store", dest="capsize",
        default=0, metavar="CAP SIZE (MBytes)",
        help="Cap mongodb collections to the given size")
    parser.add_option(
        "-f", action="append", dest="formats",
        default=[],
        help=("Print results using the given format string (See PEP 3101)\n"+
              "You can specify this option several times.\n"+
              "If format starts with [topic]:, it is applied only for the given topic\n"+
              "(e.g. \"[loc]...\" will only apply to location messages)"))
    parser.add_option(
        "-m", action="store", dest="mongouri",
        default="",
        help="MongoDB URI connection string (e.g. -m \"$MONGODB_URI\")")
    parser.add_option(
        "-n", "--numprocs", action="store", dest="numprocs",
        default=DEFAULT_PROCS, metavar="NUMBER_OF_PROCS",
        help="Number of transform processes to run")
    parser.add_option(
        "-p", action="store_true", dest="pprint",
        default=False,
        help="Pretty-print results (add newlines, indentation, etc)")
    parser.add_option(
        "-q", action="store_true", dest="quiet",
        default=False,
        help="Be quiet - do not print anything to stdout")
    parser.add_option(
        "-r", "--rate", action="store", dest="rate",
        default=0,
        help="Rate-limit input - Useful for inline processing\n"+
             "Set the maximum number of buffered chunks per process\n"+
             "before the application starts dropping chunks")
    parser.add_option(
        "-s", "--chunksize", action="store", dest="chunksize",
        default=DEFAULT_CHUNK, metavar="CHUNK_SIZE",
        help="Size of chunk for each process")
    options, args = parser.parse_args()

    # Get the number of procs, events and chunks from options parameters
    numprocs = _bounded(options.numprocs, DEFAULT_PROCS, MAX_PROCS)
    chunksize = _bounded(options.chunksize, DEFAULT_CHUNK, MAX_CHUNK)
    backoff = _bounded(options.backoff, DEFAULT_BACKOFF, MAX_RETRIES*DEFAULT_BACKOFF)
    rate = options.rate
    if rate != 0:
        rate = _bounded(rate, DEFAULT_RATE, MAX_RATE)
    mongouri = options.mongouri

    events = events_from_lines(fileinput.input(args))
    # Get the factory to use: MongoDB, text or JSON
    if mongouri == "":
        # If no formats, use JSON factory
        if len(options.formats) <= 0:
            factory = functools.partial(json_factory, options.pprint)
        # If there are formats, use text factory.
        # formats can be prefixed with "[topic]", to apply
        # to a single topic.
        # First format will be applied to any topic without its own
        # format
        else:
            formats, defaults = dict(), None
            for topic_format in options.formats:
                if not topic_format.startswith("["):
                    defaults = topic_format
                    continue
                parts = topic_format[1:].split("]", 1)
                if len(parts) < 2:
                    defaults = topic_format
                    continue
                formats[parts[0]] = parts[1]
            factory = functools.partial(text_factory, formats, defaults)
    else:
        capsize = int(options.capsize) << 20 # MBytes to bytes
        factory = functools.partial(mongo_factory, mongouri, capsize)
        # The mongo factory does not expect individual events, but lists
        # of events. We have to wrap the event iterator,
        # and set chunksize=1 so that the process pool does not chunk again.
        events = _chunk(events, chunksize)
        chunksize = 1
    # Reate-limit consumer and producer
    if rate > 0:
        factory = Limiter(numprocs, rate, factory)
        events = factory.events(events)
    # And run the pool
    quiet = options.quiet
    for result in parallel(events, factory, numprocs, chunksize, backoff):
        if not quiet:
            print(result)

if __name__ == "__main__":
    _cmd()
