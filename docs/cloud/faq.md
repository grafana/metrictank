---
title: FAQ
weight: 3
---
# FAQ

### Can I use tags?

Yes, our platform supports graphite tags as well as [meta tags](https://grafana.com/blog/2019/04/09/metrictank-meta-tags/), allowing to add extra metadata tags your series.

### Can I import my existing data?

You can import pre-existing data into the hosted platform, from either a Graphite or metrictank installation.
We either provide you with the tools and instructions, or if provided access, we offer this service for a hands-off experience.
Grafana dashboards can also be imported if you choose to use a hosted Grafana instance.

### How do I send data to the service?

See [data ingestion]({{< relref "data-ingestion" >}}).

### How does this compare to stock graphite?

The hosted platform is built on top of [metrictank](/oss/metrictank) and [graphite](/oss/graphite)
Important differences with stock Graphite to be aware of:

* support for meta tags
* the platform is optimized for append-only workloads. While historical data can be imported, we generally don't support out of order writes.
* timeseries can change resolution (interval) over time, they will be merged automatically.

## Do I have to use hosted grafana or exclusively the hosted platform?

No, the hosted platform is a datasource that you can use however you like. E.g. in combination with other datasources, and queried from any Grafana instance or other client.