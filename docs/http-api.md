# HTTP api

This documents endpoints aimed to be used by users. For internal clustering http endpoints, which may change, refer to the source.

- Note that some of the endpoints rely on being a fed a proper Org-Id.  See [Multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md).

- For GET requests, any parameters not specified as a header can be passed as an HTTP query string parameter.

## Get app status

```
GET /
POST /
```

returns:

* `200 OK` if the node is [ready](clustering.md#priority-and-ready-state)
* `503 Service not ready` otherwise.

#### Example


```bash
curl "http://localhost:6060"
```


## Walk the metrics tree and return every metric found that is visible to the org as a sorted JSON array

```
GET /metrics/index.json
POST /metrics/index.json
```

* header `X-Org-Id` required

Returns metrics stored under the given org, as well as public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md))

#### Example


```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/metrics/index.json"
```

## Find all metrics visible to the given org

```
GET /metrics/find
POST /metrics/find
```

* header `X-Org-Id` required
* query (required): can be an id, and use all graphite glob patterns (`*`, `{}`, `[]`, `?`)
* format: json, treejson, completer, pickle, or msgpack. (defaults to json)
* jsonp

Returns metrics which match the query and are stored under the given org or are public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md))
the completer format is for completion UI's such as graphite-web.
json and treejson are the same.

#### Example

```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/metrics/find?query=statsd.fakesite.counters.session_start.*.count"
```

## Deleting metrics

This will delete any metrics (technically metricdefinitions) matching the query from the index.
Note that unlike find and render patterns, these queries are recursive.
So if the delete query matches a branch, every series under that branch will be deleted.
Note that the data stays in the datastore until it expires.
Should the metrics enter the system again with the same metadata, the data will show up again.

```
POST /metrics/delete
```

* header `X-Org-Id` required
* query (required): can be a metric key, and use all graphite glob patterns (`*`, `{}`, `[]`, `?`)

#### Example

```bash
curl -H "X-Org-Id: 12345" --data query=statsd.fakesite.counters.session_start.*.count "http://localhost:6060/metrics/delete"
```

## Graphite query api

Graphite-web-like api. It can return JSON, pickle or messagepack output

```
GET /render
POST /render
```

* header `X-Org-Id` required
* maxDataPoints: int (default: 800)
* target: mandatory. one or more metric names or patterns, like graphite.
* from: see [timespec format](#tspec) (default: 24h ago) (exclusive)
* to/until : see [timespec format](#tspec)(default: now) (inclusive)
* format: json, msgp, pickle, or msgpack (default: json)
* meta: use 'meta=true' to enable metadata in response (performance measurements)
* process: all, stable, none (default: stable). Controls metrictank's eagerness of fulfilling the request with its built-in processing functions
  (as opposed to proxying to the fallback graphite).
  - all: process request without fallback if we have all the needed functions, even if they are marked unstable (under development)
  - stable: process request without fallback if we have all the needed functions and they are marked as stable.
  - none: always defer to graphite for processing.

  If metrictank doesn't have a requested function, it always proxies to graphite, irrespective of this setting.

Data queried for must be stored under the given org or be public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md))

#### Example

```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/render?target=statsd.fakesite.counters.session_start.*.count&from=3h&to=2h"
```

## Get Cluster Status

```
GET /node
```

returns a json document with the following fields:

* "name": the node name
* "primary": whether the node is a primary node or not
* "primaryChange": timestamp of when the primary state last changed
* "version": metrictank version
* "state": whether the node is [ready](clustering.md#priority-and-ready-state) to handle requests or not
* "stateChange": timestamp of when the state last changed
* "started": timestamp of when the node started up

#### Example

```bash
curl "http://localhost:6060/node"
```

## Set Cluster Status

```
POST /node
```

parameter values :

* `primary true|false`

Sets the primary status to this node to true or false.

#### Example

```bash
curl --data primary=true "http://localhost:6060/node"
```

## Analyze instance priority

```
GET /priority
```

Gets all enabled plugins to declare how they compute their
priority scores.

#### Example

```bash
curl -s http://localhost:6060/priority | jsonpp
[
    "carbon-in: priority=0 (always in sync)",
    {
        "Title": "kafka-mdm:",
        "Explanation": {
            "Explanation": {
                "Status": {
                    "0": {
                        "Lag": 1,
                        "Rate": 26494,
                        "Priority": 0
                    },
                    "1": {
                        "Lag": 2,
                        "Rate": 24989,
                        "Priority": 0
                    }
                },
                "Priority": 0,
                "Updated": "2018-06-06T11:56:24.399840121Z"
            },
            "Now": "2018-06-06T11:56:25.016645631Z"
        }
    }
]
```

## Cache delete

```
GET /ccache/delete
POST /ccache/delete
```

* `X-Org-Id`: required
* patterns: one or more query (glob) patterns. Use `**` to mean "all data" (full reset)
* expr: tag expressions
* propagate: whether to propagate to other cluster nodes. true/false

Remove chunks from the cache for matching series, or wipe the entire cache

#### Example

```bash
curl -v -X POST -d '{"propagate": true, "orgId": 1, "patterns": ["**"]}' -H 'Content-Type: application/json' http://localhost:6060/ccache/delete
```

## Get Meta Records

```
GET /metaTags
```

Returns the list of meta tag records that currently exist. At the moment these records
have no effect, but in the future they will be used to associated "virtual" tags with
metrics based on defined rules.

## Adding, Updating, Deleting Meta Records

```
POST /metaTags/upsert
```

This route can be used to create, update and delete meta tag records. Each record is
identified by its set of query expressions, if a record gets posted to this URL which
has a set of expressions that doesn't exist yet, a new meta record gets created. If
its set of query expressions already exists, the existing meta record will be updated.
If the new record has no meta tags associated with it, then an existing record with the
same set of expressions gets deleted.

## Example

```
~$ curl -s -H 'X-Org-Id: 1' http://localhost:6070/metaTags | jq
[]
~$ curl -s -H 'X-Org-Id: 1' http://localhost:6070/metaTags/upsert -H 'Content-Type: application/json' -d '{"metaTags": ["mytag=value"], "expressions": ["a=b", "c=d"]}' | jq
{
  "metaTags": [
    "mytag=value"
  ],
  "expressions": [
    "a=b",
    "c=d"
  ],
  "created": true
}
~$ curl -s -H 'X-Org-Id: 1' http://localhost:6070/metaTags | jq
[
  {
    "metaTags": [
      "mytag=value"
    ],
    "expressions": [
      "a=b",
      "c=d"
    ]
  }
]
~$ curl -s -H 'X-Org-Id: 1' http://localhost:6070/metaTags/upsert -H 'Content-Type: application/json' -d '{"metaTags": [], "expressions": ["a=b", "c=d"]}' | jq
{
  "metaTags": [],
  "expressions": [
    "a=b",
    "c=d"
  ],
  "created": false
}
mst@mst-nb1:~$ curl -s -H 'X-Org-Id: 1' http://localhost:6070/metaTags | jq
[]
```

The optional boolean parameter "propagate" tells the receiving node that this
upsert request needs to be propagated among all cluster nodes. Then the request
would look like this:

```
~$ curl -s -H 'X-Org-Id: 1' http://localhost:6070/metaTags/upsert -H 'Content-Type: application/json' -d '{"metaTags": ["mytag=value"], "expressions": ["a=b", "c=d"], "propagate": true}' | jq
```

## Batch updating all Meta Tag Records

```
POST /metaTags/swap
```

This route is an alternative to the above "upsert". It accepts a list of meta tag records
and replaces all existing records with the new ones. This is useful for users who first
generate all of their meta tag records and then want to simply replace all the records in
Metrictank with the generated ones.

## Example

```
~$ curl -s -H 'X-Org-Id: 1' 'http://localhost:6070/metaTags/swap' -H 'Content-Type: application/json' -d '{"propagate": true, "records":[{"metaTags": ["meta=tag"], "expressions": ["name=~.*[2-7]$"]}]}' | jq
{
  "local": {
    "deleted": 0,
    "added": 0
  },
  "peerResults": {
    "metrictank0": {
      "deleted": 0,
      "added": 1
    },
    "metrictank1": {
      "deleted": 0,
      "added": 1
    },
    "metrictank2": {
      "deleted": 0,
      "added": 1
    },
    "metrictank3": {
      "deleted": 0,
      "added": 1
    }
  },
  "peerErrors": null
}
```

## Misc

### Tspec

The time specification is used throughout the http api and it can be any of these forms:

* now: current time on server
* any integer: unix timestamp
* `-offset` or `offset` gets interpreted as current time minus offset.
  Where `offset` is a sequence of one or more `<num><unit>` where unit is one of:

	- ``, `s`, `sec`, `secs`, `second`, `seconds`
	- `m`, `min`, `mins`, `minute`, `minutes`
	- `h`, `hour`, `hours`
	- `d`, `day`, `days`
	- `w`, `week`, `weeks`
	- `mon`, `month`, `months`
	- `y`, `year`, `years`

* datetime in any of the following formats: `15:04 20060102`, `20060102`, `01/02/06`

