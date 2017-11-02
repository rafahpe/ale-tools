# ale-tools
Set of ALE tools - dump to json, import to databases, etc.

## feed_push

**feed-push** is a command line tool to read events from the ALE built-in *feed-reader*, and dump them as JSON, text, or push them to mongoDB.

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

To dump as text, you must specify a format for the output lines with the '*-f*' flag.

- The format string has the same syntax as python's *format* method. E.g. you can access an event properties ("Timestamp is {timestamp}"), or sub-properties ("Received visibility_rec event for client {client_ip.addr}")
  For more information on formatting, see python's [PEP 3101](https://www.python.org/dev/peps/pep-3101)
- You can specify several format strings, repeating the *-f* flag. Prefix the string with *[topic]* to apply the format just to a particular topic, e.g. "[location]Location coordinates: {sta_location_x}, {sta_location_y}"
- The last *-f* parameter without a topic prefix is used as default format string for other topics.

```bash
feed-reader -e tcp://<ALE server>:7779 | python feed_push.py \
   -f "[webcc_info]INSERT INTO wcc (ts, ccmd5, url) VALUES ('{timestamp}', '{cc_md5}', '{url}');" \
   -f "[visibility_rec]INSERT INTO vr (ts, client, ccmd5) VALUES ('{timestamp}', '{client_ip.addr}', '{cc_md5}');" \
   -f "INSERT INTO other (ts, topic) VALUES ('{timestamp}';'{topic}');"
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

```js
db.location.aggregate([
  { $match: {
      $and: [
        { datetime: { $gte: ISODate("2017-09-03 16:00:00") } },
        { datetime: { $lte: ISODate("2017-09-03 18:00:00") } },
        { opcode: { $gte: 0 }}
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
  }
])
```

And this aggregation yields the sequence of Station events in a given interval:

```js
db.station.aggregate([
  { $match: {
      $and: [
        { datetime: { $gte: ISODate("2017-09-03 16:00:00") } },
        { datetime: { $lte: ISODate("2017-09-03 18:00:00") } }
      ]
    },
  },
  { $sort: { 
      hashed_sta_eth_mac: 1,
      timestamp: 1
    }
  },
  { $project: {
      hashed_sta_eth_mac: 1,
      timestamp: 1,
      datetime: 1,
      opcode: 1,
      op: 1,
      bssid: 1,
      ap_name: 1
    }
  },
  { $group: {
      _id: {
        mac: "$hashed_sta_eth_mac"
      },
      events: { $push: "$$ROOT" }
    }
  }
], { allowDiskUse: true }) 
```
