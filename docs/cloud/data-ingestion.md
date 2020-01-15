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


## Forwarding traffic from carbon-relay to carbon-relay-ng

### Sending all your traffic to carbon-relay-ng instead of carbon-relay

The most simple way to get your traffic into carbon-relay-ng is to change the value of the `relay.DESTINATIONS` in your `carbon.conf` to the ip and port where the carbon-relay-ng's carbon input is listening. This will make the carbon-relay forward all your metrics to the defined carbon-relay-ng instance. 

For example if your carbon-relay-ng is running on the host `crng-host` and it's listening on the default port `2003`, then in your `carbon.conf` you should have this setting:
```
[relay]
DESTINATIONS = crng-host:2003
```

### Duplicating your traffic to carbon-relay-ng

#### With a single relay destination

If your current carbon-relay is forwarding to only one destination (`relay.DESTINATIONS` only has one entry), or if there are multiple destinations and your `relay.REPLICATION_FACTOR` equals the number of destinations (data gets replicated to every destination), then you can simply add the carbon-relay-ng's carbon input to this list and increase the `relay.REPLICATION_FACTOR` setting by `1`. This will make carbon-relay send every data point once to every listed destinations. For the replication factor setting to take effect the `relay.RELAY_METHOD` setting must be set to `consistent-hashing`.

For example if your carbon-relay-ng is running on the host `crng-host` and it's listening on the default port `2003` and your other destination is `other-host:2003`, then in your `carbon.conf` should look like this:

```
[relay]
DESTINATIONS = crng-host:2003,other-host:2003
REPLICATION_FACTOR = 2
RELAY_METHOD = consistent-hashing
```

Or if your current setup already has `2` destinations and a `REPLICATION_FACTOR` of `2`, then after adding carbon-relay-ng and increasing the replication factor, the `carbon.conf` would look like this:

```
[relay]
DESTINATIONS = crng-host:2003,other-host1:2003,other-host2:2003
REPLICATION_FACTOR = 3
RELAY_METHOD = consistent-hashing
```

#### With a sharded cluster of multiple relay destinations

If your current carbon-relay is already forwarding to multiple destinations (`relay.DESTINATIONS` has multiple entries) and the `relay.REPLICATION_FACTOR` is less then the number of destinations, then there is no easy way to make carbon-relay send a copy of each data point to carbon-relay-ng. 
In this case the recommended setup is to add an additional instance of carbon-relay in front of your existing one, this additional carbon-relay needs to have `2` entries in the list of relay destinations where one is your current carbon-relay instance and the other is your carbon-relay-ng. By setting `relay.REPLICATION_FACTOR` to `2` the data then gets duplicated among these two destinations. 

For example if your current carbon-relay instance is listening on port `2003` of the host `carbon-host` and your carbon-relay-ng is listening on port `2003` of `crng-host` in your new carbon-relay instance's config you would need these entries:

```
[relay]
DESTINATIONS = crng-host:2003,carbon-host:2003
REPLICATION_FACTOR = 2
RELAY_METHOD = consistent-hashing
```

## High availability and scaling of carbon-relay-ng

### Scaling with carbon-relay-ng

When distributing traffic among multiple instances of carbon-relay-ng it is important to ensure that the same metrics always get sent to the same carbon-relay-ng instances to preserve the order of the data points, this can be done with consistent hashing.
The carbon-relay and carbon-relay-ng daemons both support consistent hashing to distribute traffic among its carbon destinations. If the carbon-relay(-ng) instance which does the consistent hashing itself becomes a bottleneck it is also ok to have multiple carbon-relay(-ng) instances running in parallel and doing the consistent hashing, among these instances the traffic may then be round-robed.

Example which uses carbon-relay to do the consistent hashing before forwarding the carbon traffic to multiple carbon-relay-ng instances:
```
            incoming carbon traffic
                       |
                 <round-robin>
                /             \
    |----------------|    |----------------|
    | carbon-relay-1 |    | carbon-relay-2 |
    |----------------|    |----------------|
             |                     |
  <consistent hashing>  <consistent hashing>
             |      \    /         |
             |       \  /          |
             |        \/           |
             |        /\           |
             |       /  \          |
             |      /    \         |
|-------------------|    |-------------------|
| carbon-relay-ng-1 |    | carbon-relay-ng-2 |
|-------------------|    |-------------------|
                    \    /
               |--------------|
               | GrafanaCloud |
               |--------------|
```

### Failure tolerance with carbon-relay-ng

As mentioned in the previous chapter, it is important that metrics get distributed among carbon-relay-ng instances in a consistent way to preserve the order of the datapoints. However, it is ok if the data gets duplicated among carbon-relay-ng instances to achieve failure tolerance, whichever copy of a given data point gets ingested by GrafanaCloud first will be accepted and the second copy will be rejected. Keep in mind that this will double the internet bandwidth used to send the data to GrafanaCloud.
Currently only the carbon-relay daemon supports duplicating the data in combination with consistent hashing. This can be achieved by using the `relay.REPLICATION_FACTOR` parameter. In the example scenario of the previous chapter, the carbon-relay config files would then need to contain these settings:

```
DESTINATIONS = carbon-relay-ng-1:<port>,carbon-relay-ng-2:<port>
REPLICATION_FACTOR = 2
RELAY_METHOD = consistent-hashing
```

This can also be scaled up to more than only `2` destinations, while the `REPLICATION_FACTOR` may be left at `2` to only create two copies of each data point. For example this is valid too:

```
DESTINATIONS = carbon-relay-ng-1:<port>,carbon-relay-ng-2:<port>,carbon-relay-ng-3:<port>,carbon-relay-ng-4:<port>
REPLICATION_FACTOR = 2
RELAY_METHOD = consistent-hashing
```