# ale-tools
Set of ALE tools - dump to json, import to databases, etc.

## feed_push

**feed-push** is a command line tool to read events from the ALE built-in *feed-reader*, and dump them as JSON or push them to mongoDB.

For instance, to dump events as json (one per line):

```bash
# Save events to a file, for reading them later
feed-reader -e tcp://<ALE server>:7779 > messages.log

# Read events from a file (or several files, you can list many)
python feed_push.py messages.log

# Read events straight from feed-reader
feed-reader -e tcp://<ALE server>:7779 | python feed_push.py
```

To pretty-print json events, add the '*-p*' flag:

```bash
feed-reader -e tcp://<ALE server>:7779 | python feed_push.py -p
```

To push events to a Mongo DB, use the '*-m*' flag:

```bash
feed-reader -e tcp://<ALE server>:7779 | python feed_push.py \
   -m "mongodb:<username>:<pass>@mongodb-uri"
```

Remember to URL-escape characters in your password. Events are stored in a mongoDB called **ale**, with one collection per topic.

You can tell *feed_push* to try to create capped collections with the '*-c*' flag, and a size (in megabytes):

```bash
feed-reader -e tcp://<ALE server>:7779 | python feed_push.py \
   -m "mongodb:<username>:<pass>@mongodb-uri" -c 32
```
