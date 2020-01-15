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

## Using carbon-relay-ng as a replacement for carbon-relay or carbon-cache

The most simple way to send your carbon traffic to GrafanaCloud is to use carbon-relay-ng as a replacement for your current carbon-relay or carbon-cache. Carbon-relay-ng has a carbon input which supports the plain text and the pickle protocols, just like carbon-relay and carbon-cache. 

An example configuration to do that can be downloaded from your Hosted Metrics instance's "Details" page. It contains a `grafanaNet` route pointing at your instance.

## Sending a copy of your carbon traffic to GrafanaCloud

It is possible to duplicate your data to send one copy to your existing Graphite infrastructure and the other to GrafanaCloud. To do this you can put an instance of carbon-relay-ng in front of your existing carbon-relay or carbon-cache and make it duplicate the traffic. Carbon-relay-ng allows you to specify routes of various types, to send a copy to GrafanaCloud you need to add a route of the type `grafanaNet`, to send a copy to your existing carbon-relay/carbon-cache you can add a carbon route.

For example if you currently have carbon-relay listening on port `2003` and all of your infrastructure is sending its carbon traffic there, you could change it to listen on port `2053` instead and then start a carbon-relay-ng on port `2003` with this config to send a copy of the traffic to `localhost:2053`:

```
[[route]]
key = 'carbon'
type = 'sendAllMatch'
destinations = [
    'localhost:2053 spool=true pickle=false'
]

[[route]]
key = 'grafanaNet'
type = 'grafanaNet'
addr = 'https://<Your endpoint address>'
apikey = '<Your Grafana.com API Key>'
schemasFile = '/etc/carbon-relay-ng/storage-schemas.conf'
```

# High availability and scaling of carbon-relay-ng

## Scaling with carbon-relay-ng

When distributing traffic among multiple instances of carbon-relay-ng it is important to ensure that the same metrics always get sent to the same carbon-relay-ng instances to preserve the order of the data points, this can be done using consistent hashing.
The carbon-relay and carbon-relay-ng daemons both support consistent hashing to distribute traffic among its carbon destinations. If the carbon-relay(-ng) instance which does the consistent hashing itself becomes a bottleneck it is also ok to have multiple carbon-relay(-ng) instances running in parallel and doing the consistent hashing, among these instances the traffic may then be round-robed.

Example which uses carbon-relay-ng to do the consistent hashing before forwarding the carbon traffic to multiple other carbon-relay-ng instances:
```
            incoming carbon traffic
                       |
                 <round-robin>
                /             \
|-------------------|    |-------------------|
| carbon-relay-ng-1 |    | carbon-relay-ng-2 |
|-------------------|    |-------------------|
             |                     |
  <consistent hashing>  <consistent hashing>
             |      \    /         |
             |       \  /          |
             |        \/           |
             |        /\           |
             |       /  \          |
             |      /    \         |
|-------------------|    |-------------------|
| carbon-relay-ng-3 |    | carbon-relay-ng-4 |
|-------------------|    |-------------------|
                    \    /
               |--------------|
               | GrafanaCloud |
               |--------------|
```

To use consistent hashing in carbon-relay-ng configure a carbon route with multiple destinations and set the type to `consistentHashing`. For the above example the route would look like this:

```
[[route]]
key = 'consistent-hashing'
type = 'consistentHashing'
destinations = [
  'carbon-relay-ng-3:2001',
  'carbon-relay-ng-4:2001'
]
```

## Failure tolerance with carbon-relay-ng

As mentioned in the previous chapter, it is important that metrics get distributed among carbon-relay-ng instances in a consistent way to preserve the order of the datapoints. However, it is ok if the data gets duplicated among carbon-relay-ng instances to achieve failure tolerance, whichever copy of a given data point gets ingested by GrafanaCloud first will be accepted and the second copy will be rejected. Keep in mind that this will double the internet bandwidth used to send the data to GrafanaCloud.

With-carbon-relay-ng it is possible to duplicate traffic by adding multiple routes. To have one cluster of carbon-relay-ng instances in each of two different availability zones and send one copy of every data point to each availability zone you need to create two routes where each sends the data to one availability zone and does consistent hashing within the instances of that availability zone. The consistent hashing is necessary to prevent that data gets out of order.

For example if you have two instances of carbon-relay-ng called `carbon-relay-ng-1`/`carbon-relay-ng-2` running in availability zone `availability-zone-1` and another two called `carbon-relay-ng-3`/`carbon-relay-ng-4` running in availability zone `availability-zone-2` then your carbon-relay-ng config would need to have these two routes:

```
[[route]]
key = 'availability-zone-1'
type = 'consistentHashing'
destinations = [
  'carbon-relay-ng-1:2001',
  'carbon-relay-ng-2:2001'
]

[[route]]
key = 'availability-zone-2'
type = 'consistentHashing'
destinations = [
  'carbon-relay-ng-3:2001',
  'carbon-relay-ng-4:2001'
]
```

This setup gives you redundancy because each availability zone has one copy of the data and it also gives you scalability because within each availability zone you can scale up the cluster by adding more entries to the `destinations` parameter.