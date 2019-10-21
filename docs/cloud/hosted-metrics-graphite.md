---
title: Hosted Metrics - Graphite
header: false
---

# Hosted Metrics - Graphite

Grafana Labs' Hosted metrics Graphite service offers a graphite-compatible monitoring backend as a service.
It acts and behaves as a regular graphite datasource within Grafana (or other tools), but behind the scenes, it is a sophisticated platform run by a team of dedicated engineers.

* [data ingestion](#data-ingestion)
* [http api](#http-api)
* [faq](#faq)

---

## Data Ingestion

We support:
* [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng), which is a graphite carbon relay, which supports aggregations and sending data to our endpoint over a secure, robust transport.
* custom tools that use our API. See our [golang, python and shell examples](https://github.com/grafana/hosted-metrics-sender-example)
* direct carbon input. This is discouraged though, as it is not reliable over the internet and not secure.

The recommended and most popular option is using carbon-relay-ng.
Customers typically deploy using either of these 2 options:

* run the relay as an extra component external to your existing graphite pipeline. Data can be directed to it from any existing carbon relay.
* replace an existing carbon-relay with carbon-relay-ng

If your Graphite stack does not currently contain any relay, then you can simply add carbon-relay-ng, have your clients (statsd, collectd, diamond, etc) send data to the relay, which in turn can send data to your existing graphite server *and* to our platform.

When creating a Hosted Metrics Graphite instance, we provide a carbon-relay-ng config file that you can plug in and be ready to use out of the box.
We also have Grafana Labs engineers ready to advise further on set up, if needed.

---

## HTTP API

The HTTP API is the same as that of Graphite, with the addition of ingestion, authentication and meta tags.

First of all, there are two endpoints you will be talking to. They are provided in your grafana.com Hosted Metrics instance UI.
They will look something like:

* `<base_in>` : `https://tsdb-<id>.hosted-metrics.grafana.net/metrics`
* `<base_out>` : `https://tsdb-<id>.hosted-metrics.grafana.net/graphite`

Furthermore, you will need to provision API keys to talk to the API. Each key will be of one of these types:

* Viewer
* MetricsPublisher
* Editor
* Admin


## Common Request Parameters

Many of the API methods involve using Graphite patterns (queries), tag queries and the standard Graphite from/to syntax.

### Graphite Patterns

[Graphite patterns](https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards) are queries that involve glob patterns (`*`, `{}`, `[]`, `?`).

### Tag Expressions

Tags expressions are strings, and may have the following formats:

```
tag=spec    tag value exactly matches spec
tag!=spec   tag value does not exactly match spec
tag=~value  tag value matches the regular expression spec
tag!=~spec  tag value does not match the regular expression spec
```

Any tag spec that matches an empty value is considered to match series that don’t have that tag, and at least one tag spec must require a non-empty value.
Regular expression conditions are treated as being anchored at the start of the value.

### From/To (Until)

[Graphite from/to](https://graphite.readthedocs.io/en/latest/render_api.html#from-until)

## Endpoints


### Adding New Data: Posting To `/metrics`

The main entry point for any publisher to publish data to, be it [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng/), or any other script or application such as the [hosted-metrics-sender-example](https://github.com/grafana/hosted-metrics-sender-example)

* Method: POST
* API key type: any (including Viewer)

#### Headers

* `Authorization: Bearer <api-key>` required
* `Content-Type`: supports 3 values:
  - `application/json`: the simplest one, and the one used here
  - `rt-metric-binary`: same datastructure, but messagepack encoded. (see [the MetricData Marshal/Encode methods](https://godoc.org/github.com/grafana/metrictank/schema#MetricData))
  - `rt-metric-binary-snappy`: same as above, but snappy compressed.

#### Data Format

Each metricpoint message can have the following properties:
```
name     // graphite style name (required)
interval // the resolution of the metric in seconds (required)
value    // float64 value (required)
time     // unix timestamp in seconds (required)
tags     // list of key=value pairs of tags (optional)
```

#### Example

```sh
timestamp_now_rounded=$(($(date +%s) / 10 * 10))
timestamp_prev_rounded=$((timestamp_now_rounded - 10))

curl -X POST -H "Authorization: Bearer $key" -H "Content-Type: application/json" "$out" -d '[{
    "name": "test.metric",
    "interval": 10,
    "value": 12.345,
    "time": '$timestamp_prev_rounded'
},
{
    "name": "test.metric",
    "interval": 10,
    "value": 12.345,
    "time": '$timestamp_now_rounded'
},
{
    "name": "test.metric.tagged",
    "interval": 10,
    "value": 1,
    "tags": ["foo=bar", "baz=quux"],
    "time": '$timestamp_prev_rounded'
},
{
    "name": "test.metric.tagged",
    "interval": 10,
    "value": 2,
    "tags": ["foo=bar", "baz=quux"],
    "time": '$timestamp_now_rounded'
}
]'

```

### Deleting Metrics
#### Non-tagged With `/metrics/delete`

Deletes metrics which match the `query` and all child nodes.

Note that unlike the find and render patterns, these queries are recursive.
So if the delete query matches a branch, **every** series under that branch will be deleted.

* Method: POST
* API key type:

##### Parameters

* user name: `api_key`
* password: Your Grafana.com API Key
* query (required): [Graphite pattern] (#graphite-patterns)

##### Example

```sh
curl -u "api_key:<Your Grafana.com API Key>" https://<tsdbgw>/metrics/delete -d query=some.series.to.delete.*
```

### Finding Metrics
#### Non-tagged With `/metrics/find`

Returns metrics which match the `query` and have received an update since `from`.

* Method: GET or POST
* API key type: any (including MetricsPublisher)

##### Headers

* `Authorization: Bearer <api-key>` required

##### Parameters

* query (required): [Graphite pattern](#graphite-patterns)
* format: json, treejson, completer, pickle, or msgpack. (defaults to json)
* jsonp: true/false: enables jsonp
* from: Graphite from time specification (defaults to now-24hours)

##### Output formats

* json, treejson (default/unspecified): the standard format
* completer: used for graphite-web's completer UI
* msgpack: optimized transfer format
* pickle: deprecated


##### Example

```sh
curl -H "Authorization: Bearer $key" "$base_out/metrics/find?query=metrictank"
[
    {
        "allowChildren": 1,
        "expandable": 1,
        "leaf": 0,
        "id": "metrictank",
        "text": "metrictank",
        "context": {}
    }
]
```

The response indicates that there are metric names that live under the "metrictank" term (it is expandable)
and there is no data under the name "metrictank" (it is not a leaf node).

So we update the query to see what we can expand to:

```sh
curl -H "Authorization: Bearer $key" "$base_out/metrics/find?query=metrictank.*"
[
    {
        "allowChildren": 1,
        "expandable": 1,
        "leaf": 0,
        "id": "metrictank.aggstats",
        "text": "aggstats",
        "context": {}
    }
]
```

The response for the updated query shows which data lives under the "metrictank" name, in this case the tree extends under "metrictank.aggstats".

As we continue to dig deeper into the tree, by updating our query based on what we get back, we eventually end up at the leaf:

```sh
curl -H "Authorization: Bearer $key" "$out/metrics/find?query=metrictank.aggstats.*.tank.metrics_active.gauge32"
[
    {
        "allowChildren": 0,
        "expandable": 0,
        "leaf": 1,
        "id": "metrictank.aggstats.us-east2-id-name.tank.metrics_active.gauge32",
        "text": "gauge32",
        "context": {}
    }
]
```

#### Tagged With `/tags/findSeries`

Returns metrics which match tag queries and have received an update since `from`.
Note: the returned results are not deduplicated and in certain cases it is possible
that duplicate entries will be returned.

* Method: GET or POST
* API key type: any (including MetricsPublisher)

##### Headers

* `Authorization: Bearer <api-key>` required

##### Parameters

* expr (required): a list of [tag expressions](#tag-expressions)
* from: Graphite [from time specification](#fromto) (optional. defaults to now-24hours)

##### Example

```sh
curl -H "Authorization: Bearer $key" "$out/tags/findSeries?expr=datacenter=dc1&expr=server=web01"

[
  "disk.used;datacenter=dc1;rack=a1;server=web01"
]
```

### Render `/render` (return data for a given query)

Graphite-web-like api. It can return JSON, pickle or messagepack output

* Method: GET or POST (recommended. as GET may result in too long URL's)
* API key type: any (viewer, publisher, editor)

##### Headers

* `Authorization: Bearer <api-key>` required


##### Parameters

* maxDataPoints: int (default: 800)
* target: mandatory. one or more metric names or [patterns](#graphite-patterns).
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
curl -H "Authorization: Bearer $key" "http://localhost:6060/render?target=statsd.fakesite.counters.session_start.*.count&from=3h&to=2h"
```


---

## FAQ

### Can I use tags?

Yes, our platform supports graphite tags as well as [meta tags](https://grafana.com/blog/2019/04/09/metrictank-meta-tags/), allowing to add extra metadata tags your series.

### Can I import my existing data?

You can import pre-existing data into the hosted platform, from either a Graphite or metrictank installation.
We either provide you with the tools and instructions, or if provided access, we offer this service for a hands-off experience.
Grafana dashboards can also be imported if you choose to use a hosted Grafana instance.

### How do I send data to the service?

See [data ingestion](#data-ingestion)

### How does this compare to stock graphite?

The hosted platform is built on top of [metrictank](/oss/metrictank) and [graphite](/oss/graphite)
Important differences with stock Graphite to be aware of:

* support for meta tags
* the platform is optimized for append-only workloads. While historical data can be imported, we generally don't support out of order writes.
* timeseries can change resolution (interval) over time, they will be merged automatically.

## Do I have to use hosted grafana or exclusively the hosted platform?

No, the hosted platform is a datasource that you can use however you like. E.g. in combination with other datasources, and queried from any Grafana instance or other client.
