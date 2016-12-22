# HTTP api

This documents endpoints aimed to be used by users. For internal clustering http endpoints, which may change, refer to the source.

- Note that some of the endpoints rely on being a fed a proper Org-Id. You may not want to expose directly to people if they can control that header. Instead, you may want to run [graphite-metrictank](https://github.com/raintank/graphite-metrictank) in front,
which will authenticate the request and set the proper header, assuring security.

- For GET requests, any parameters not specified as a header can be passed as an HTTP query string parameter.

## Get app status

```
GET /
POST /
```

returns:

* `200 OK` if the node is primary or a warmed up secondary (`warmupPeriod` has elapsed)
* `503 Service not ready` if the node is secondary and not yet warmed up.

#### Example


```bash
curl "http://localhost:6060"
```


## Walk the metrics tree and return every metric found as a sorted JSON array, for the given org (or public)

```
GET /metrics/index.json
POST /metrics/index.json
```

* header `X-Org-Id` required

If orgId is -1, returns the metrics for all orgs. (but you can't neccessarily distinguish which org they're from)
If it is not, returns metrics for the given org, as well as org -1.

#### Example


```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/metrics/index.json"
```

## Find all metrics visible to the given org (or public)

```
GET /metrics/find
POST /metrics/find
```

* header `X-Org-Id` required
* query (required): can be an id, and use all graphite glob patterns (`*`, `{}`, `[]`, `?`)
* format: json, treejson, completer. (defaults to json)
* jsonp

the completer format is for completion UI's such as graphite-web.
json and treejson are the same.

#### Example

```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/metrics/find?query=statsd.fakesite.counters.session_start.*.count"
```

## Deleting metrics

This will delete any metrics (technically metricdefinitions) matching the query from the index.
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

This is the early beginning of a graphite-web/graphite-api replacement. It only returns JSON output
This section of the api is **very early stages**.  Your best bet is to use graphite-api + graphite-metrictank in front of metrictank, for now.

```
GET /render
POST /render
```

* header `X-Org-Id` required
* maxDataPoints: int (default: 800)
* target: mandatory. one or more metric names or patterns, like graphite.  
  note: **no graphite functions are currently supported** except that
  you can use `consolidateBy(id, '<fn>')` or `consolidateBy(id, "<fn>")` where fn is one of `avg`, `average`, `min`, `max`, `sum`. see
  [Consolidation](https://github.com/raintank/metrictank/blob/master/docs/consolidation.md)
* from: see [timespec format](#tspec) (default: 24 ago) (exclusive)
* to/until : see [timespec format](#tspec)(default: now) (inclusive)

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
* "state": whether the node is ready to handle requests or not
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

