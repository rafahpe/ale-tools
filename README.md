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

Example of mongo query you can use to get the last known position of every device seen by ALE in an interval of time:

```json
db.location.aggregate([
  { $match: {
      $and: [
        { datetime: { $gte: ISODate("2017-09-03 16:00:00") } },
        { datetime: { $lte: ISODate("2017-09-03 18:00:00") } }
      ]
    },
  },
  { $sort: {
      timestamp: -1
    }
  },
  { $group: {
      _id: "$hashed_sta_eth_mac",
      datetime:       { $first: "$datetime" },
      opcode:         { $first: "$opcode" },
      campus_id:      { $first: "$campus_id" },
      floor_id:       { $first: "$floor_id" },
      sta_location_x: { $first: "$sta_location_x" },
      sta_location_y: { $first: "$sta_location_y" }
    }
  },
  { $match: {
      opcode: { $gte: 0 }
    }
  }
])
```
