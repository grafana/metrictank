# HTTP api

Note that some of the endpoints rely on being a fed a proper Org-Id.
You may not want to expose directly to people if they can control that header.
Instead, you may want to run [graphite-metrictank](https://github.com/raintank/graphite-metrictank) in front,
which will authenticate the request and set the proper header, assuring security.

## Get app status

```
GET /
POST /
```

returns:

* `200 OK` if the node is primary or a warmed up secondary (`warmupPeriod` has elapsed)
* `503 Service not ready` if the node is secondary and not yet warmed up.



## Walk the metrics tree and return every metric found as a sorted JSON array, for the given org (or public)

```
GET /metrics/index.json
POST /metrics/index.json
```

* header `X-Org-Id` required

If orgId is -1, returns the metrics for all orgs. (but you can't neccessarily distinguish which org they're from)
If it is not, returns metrics for the given org, as well as org -1.

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
  you can use `consolidateBy(id, '<fn>')` where fn is one of `avg`, `average`, `min`, `max`, `sum`. see
  [Consolidation](https://github.com/raintank/metrictank/blob/master/docs/consolidation.md)
* from: see [timespec format](#tspec) (default: 24 ago) (exclusive)
* to/until : see [timespec format](#tspec)(default: now) (inclusive)

## Low-level data query api

This query API is for applications that already know the UUID's of the metrics they're looking for.
It currently is primarily used by [graphite-metrictank](https://github.com/raintank/graphite-metrictank)
It returns JSON output in the same format as the graphite query api.

```
GET /get
POST /get
```

* header `X-Org-Id` required
* maxDataPoints: int (default: 800)
* target: mandatory. one or more UUID's of metrics. You can use `consolidateBy(id, '<fn>')` where fn is one of `avg`, `average`, `min`, `max`, `sum`. see
  [Consolidation](https://github.com/raintank/metrictank/blob/master/docs/consolidation.md)
* from: see [timespec format](#tspec)(default: 24 ago) (inclusive)
* to/until : see [timespec format](#tspec)(default: now) (exclusive)


## Cluster status

```
GET /cluster
```

returns a json document with the following fields:

* instance name
* primary status
* primary status last change timestamp

## Change primary role

```
POST /cluster
```

parameter values :

* `primary true|false`

Sets the primary status to this node to true or false.

## Misc

### Tspec

time specification used throughout the http api:
can be any of these forms:

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

