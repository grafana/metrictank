# Installation guide for RPM-based Linux (CentOS, Fedora, OpenSuse, RedHat)

## Dependencies overview

We'll go over these in more detail below.

* Cassandra. We run and recommend 3.0.8 .
  See [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)
* Our [graphite-raintank finder plugin](https://github.com/raintank/graphite-metrictank)
  and our [graphite-api fork](https://github.com/raintank/graphite-api/) (installed as 1 component)
  We're working toward simplifying this much more.
* [statsd](https://github.com/etsy/statsd) or something compatible with it.  For instrumentation
* Optional: Elasticsearch for persistence of metrics metadata.
  See [metadata in ES](https://github.com/raintank/metrictank/blob/master/docs/metadata.md#es)
* Optional: Kafka, if you want to buffer data in case metrictank goes down. Kafka 0.10 is recommended, but 0.9 should work too.

Note: Cassandra, Elasticsearch, and Kafka require Java. We recommend using Oracle Java 8.

## How things fit together

metrictank ingest metrics data. The data can be sent into it, or be read from a queue (see
[Inputs](https://github.com/raintank/metrictank/blob/master/docs/inputs.md))
metrictank will compress the data into chunks in RAM, a configurable amount of the most recent data
is kept in RAM, but the chunks are being saved to Cassandra as well.  You can use a single Cassandra
instance or a cluster.  Metrictank will also respond to queries: if the data is recent, it'll come out of
RAM, and older data is fetched from cassandra.  This happens transparantly.
Metrictank maintains an index of metrics metadata, for all series it Sees. If you want the index to be maintained
across restarts, it can use Elasticsearch to save and reload the data.
You'll typically query metrictank by querying graphite-api which uses the graphite-metrictank plugin to talk
to metrictank.  You can also query metrictank directly but this is experimental and too early for anything useful.

## Installation

We automatically build rpms and debs on circleCi for all needed components whenever the build succeeds.
These packages are pushed to packagecloud.

[Instructions to enable the raintank packagecloud repository](https://packagecloud.io/raintank/raintank/install)

You need to install these packages:

* metrictank
* graphite-metrictank (includes both our graphite-api variant as well as the graphite-metrictank finder plugin)

Releases are simply tagged versions like `0.5.1` ([releases](https://github.com/raintank/metrictank/releases)),
whereas commits in master following a release will be named `version-commit-after` for example `0.5.1-20` for
the 20th commit after `0.5.1`

We aim to keep master stable, so that's your best bet.

Supported distributions:

* Ubuntu 14.04 (Trusty Tahr), 16.04 (Xenial Xerus)
* Debian 7 (wheezy), 8 (jessie)
* Centos 6, 7

## Set up cassandra

[official instructions, for more info](http://docs.datastax.com/en/cassandra/3.x/cassandra/install/installRHEL.html)

* Add the DataStax Distribution of Apache Cassandra 3.x repository to the /etc/yum.repos.d/datastax.repo:

```
[datastax-ddc] 
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/datastax-ddc/3.1
enabled = 1
gpgcheck = 0
```

* Run `sudo yum install datastax-ddc`

For basic setups, you can just install it and start it with default settings.
To tweak schema and settings, see [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)

## Set up elasticsearch

* Install the GPG key with `rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch`

* Add the following in your /etc/yum.repos.d/ directory in a file with a .repo suffix, for example elasticsearch.repo

```
[elasticsearch-2.x]
name=Elasticsearch repository for 2.x packages
baseurl=https://packages.elastic.co/elasticsearch/2.x/centos
gpgcheck=1
gpgkey=https://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1
```

* Install elasticsearch with `yum install elasticsearch`

[more details on official page](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/setup-repositories.html)

You can start it with default settings. 

## Set up statsd

Metrictank needs statsd or a statsd-compatible agent for its instrumentation.
It will refuse to start if nothing listens on the configured `statsd-addr`.

You can install the official [statsd](https://github.com/etsy/statsd) (see its installation instructions)
or an alternative. We recommend [vimeo/statsdaemon](https://github.com/vimeo/statsdaemon).

For the [metrictank dashboard](https://grafana.net/dashboards/279) to work properly, you need the right statsd/statsdaemon settings.

Below are instructions for statsd and statsdaemon:

Note:
 * `<environment>` is however you choose to call your environment. (test, production, dev, ...).
 * we recommend installing statsd/statsdaemon on the same host as metrictank.

### Statsdaemon

[Statsdaemon](https://github.com/vimeo/statsdaemon) is the recommended option.
To install it, you can either use the deb packages from the aforementioned repository,
or you need to have a [Golang](https://golang.org/) compiler installed.
In that case just run `go get github.com/Vimeo/statsdaemon/statsdaemon`

Get the default config file from `https://github.com/vimeo/statsdaemon/blob/master/statsdaemon.ini`
and update the following settings:

```
flush_interval = 1
prefix_rates = "stats.<environment>."
prefix_timers = "stats.<environment>.timers."
prefix_gauges = "stats.<environment>.gauges."

percentile_thresholds = "90,75"
```

Then just run `statsdaemon`.  If you use ubuntu you can use the package or the [upstart init config](https://github.com/vimeo/statsdaemon/blob/master/upstart-init-statsdaemon.conf) from the statsdaemon repo.

### Statsd

See the instructions on the [statsd homepage](https://github.com/etsy/statsd)
Set the following options:

```
flushInterval: 1000
globalPrefix: "stats.<environment>"
```

## Optional: set up kafka

You can run a persistent queue in front of metrictank.
If your metric instance(s) go down, then a queue is helpful in buffering and saving all the data while your instance(s) is/are down.
The moment your metrictank instance(s) come(s) back up, they can replay everything they missed (and more, it's useful to load in older data
so that you can serve queries for it out of RAM).
Also, in case you want to make any change to your aggregations, Cassandra cluster, or whatever, it can be useful to re-process older data.

** Note: the above actually doesn't work yet, as we don't have the seek-back-in-time implemented yet to fetch old data from Kafka.
So for now using Kafka is more about preparing for the future than getting immediate benefit. **

### Zookeeper

Kafka requires Zookeeper, so set that up first.

* Download zookeeper. Find a mirror at http://www.apache.org/dyn/closer.cgi/zookeeper/, pick a stable zookeeper, and download it to your server.

* Unpack zookeeper. For this guide we'll install it in `/opt`.

```
cd /opt
tar -zxvf /path/to/zookeeper-3.4.8.tar.gz
ln -s /opt/zookeeper-3.4.8 /opt/zookeeper
mkdir /var/lib/zookeeper
```

* Make a config file for zookeeper in `/opt/zookeeper/conf/zoo.cfg`:

```
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
```

* Start zookeeper: `/opt/zookeeper/bin/zkServer.sh start`

([more details](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html))

### Kafka

We recommend 0.10 or higher.

* Download kafka. Find a mirror at https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz, and download kafka to your server.

* Unpack kafka. Like zookeeper, we'll do so in `/opt`.

```
cd /opt
tar -zxvf /path/to/kafka_2.11-0.10.0.1.tgz
ln -s /opt/kafka_2.11-0.10.0.1 /opt/kafka
```

* Start kafka: `/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties`

([more details](https://kafka.apache.org/documentation.html#quickstart))

## Configuration

See the [example config file](https://github.com/raintank/metrictank/blob/master/metrictank-sample.ini) which guides you through the various options.

You may need to adjust the `statsd-addr` based on where you decided to run that service.

Out of the box, one input is enabled: the [Carbon line input](https://github.com/raintank/metrictank/blob/master/docs/inputs.md#carbon)
It uses a default storage-schemas to coalesce every incoming metric into 1 second resolution.  You may want to fine tune this for your needs.
(or simply what you already use in a pre-existing Graphite install).
See the input plugin documentation referenced above for more details.

If you want to use Kafka, you should enable the Kafka-mdm input plugin.  See [the Inputs docs for more details](https://github.com/raintank/metrictank/blob/master/docs/inputs.md).
See the `kafka-mdm-in` section in the config for the options you need to tweak.

## Run it!

If using upstart:
```
service metrictank start
```

If using systemd:
```
systemctl start metrictank
```

Note that metrictank simply logs to stdout.  So where the log data ends up depends on your init system.

If using upstart, you can then find the logs at `/var/log/upstart/metrictank.log`.
With systemd, you can use something like `journalctl -f metrictank`.
