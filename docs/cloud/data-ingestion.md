---
title: Data Ingestion
weight: 1
---

# Data Ingestion

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
