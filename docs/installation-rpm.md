# Installation guide for RPM-based Linux (CentOS, Fedora, OpenSuse, RedHat)

## Dependencies overview

We'll go over these in more detail below.

* Cassandra. We run and recommend 3.0.8 .
  See [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)
* Our [graphite-raintank finder plugin](https://github.com/raintank/graphite-metrictank)
  and our [graphite-api fork](https://github.com/raintank/graphite-api/) (installed as 1 component)
  We're working toward simplifying this much more.
* Optional: [statsd](https://github.com/etsy/statsd) or something compatible with it.  For instrumentation
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

## Step 1

We recommend a server with at least 8GB RAM and a few CPU's.
You need root access. All the commands shown assume you're root.


## Metrictank and graphite-metrictank

### Short version

You can enable our repository and install the packages like so:

```
curl -s https://packagecloud.io/install/repositories/raintank/raintank/script.rpm.sh | bash
yum install metrictank graphite-metrictank
```

Then just start it:

```
systemctl start graphite-metrictank
```

Logs - if you need them - will be at /var/log/graphite/graphite-metrictank.log

### Long version

We automatically build rpms and debs on circleCi for all needed components whenever the build succeeds.
These packages are pushed to packagecloud.

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

[more info](https://packagecloud.io/raintank/raintank/install)


## Set up java

Download and install the oracle java rpm:

```
wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
"http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jre-8u60-linux-x64.rpm"
yum localinstall jre-8u60-linux-x64.rpm
```

[more info](https://www.digitalocean.com/community/tutorials/how-to-install-java-on-centos-and-fedora)

## Set up cassandra

* Add the DataStax respository:

```
cat << EOF > /etc/yum.repos.d/datastax.repo
[datastax-ddc] 
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community/
enabled = 1
gpgcheck = 0
EOF
```

* Run `yum install cassandra30`

For basic setups, you can just install it and start it with default settings.
To tweak schema and settings, see [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)

* Start cassandra:

```
systemctl start cassandra
```

The log - should you need it - is at /var/log/cassandra/cassandra.log

[more info](http://docs.datastax.com/en/cassandra/3.x/cassandra/install/installRHEL.html)


## Set up elasticsearch

* Install the GPG key with `rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch`

* Add the elastic repository:

```
cat << EOF > /etc/yum.repos.d/elasticsearch.repo
[elasticsearch-2.x]
name=Elasticsearch repository for 2.x packages
baseurl=https://packages.elastic.co/elasticsearch/2.x/centos
gpgcheck=1
gpgkey=https://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1
EOF
```

* Install elasticsearch with `yum install elasticsearch`

* You can start it with default settings.

```
systemctl start elasticsearch.service
```

[more info](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/setup-repositories.html)


## Set up statsd

While optional, we highly recommend installing statsd or a statsd-compatible agent for instrumentation, so you can get insights into what's going on.
To disable, set `statsd-enabled` to false in the configuration.

Metrictank will refuse to start if `statsd-enabled` is true and nothing listens on the configured `statsd-addr`.

You can install the official [statsd](https://github.com/etsy/statsd) (see its installation instructions)
or an alternative. We recommend [raintank/statsdaemon](https://github.com/raintank/statsdaemon).

For the [metrictank dashboard](https://grafana.net/dashboards/279) to work properly, you need the right statsd/statsdaemon settings.

Below are instructions for statsd and statsdaemon.

Note:
 * `<environment>` is however you choose to call your environment. (test, production, dev, ...).
 * we recommend installing statsd/statsdaemon on the same host as metrictank.
 * Note, statsd/statsdaemon will write to metrictank's carbon port on localhost:2003, while metrictank will send its own performance metrics to statsd/statsdaemon on localhost:8125.
   This is a circular dependency, we typically just bring up statsdaemon first, and metrictank a bit later.  This means you will see some "unable to flush" errors from statsdaemon
   or statsd during the timeframe where metrictank is not up yet.

### Statsdaemon

[Statsdaemon](https://github.com/raintank/statsdaemon) is the recommended option.
Install the package from the raintank repository you enabled earlier:

```
yum install statsdaemon
```

Update the following settings in `/etc/statsdaemon.ini`:

```
flush_interval = 1
prefix_rates = "stats.<environment>."
prefix_timers = "stats.<environment>.timers."
prefix_gauges = "stats.<environment>.gauges."

percentile_thresholds = "90,75"
```

Run it:

```
systemctl start statsdaemon
```

The logs, should you need them:

```
journalctl -u statsdaemon
```

### Statsd

If you want to use the origital statsd server instead of statsdaemon,
see the instructions on the [statsd homepage](https://github.com/etsy/statsd)
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

### Zookeeper

Kafka requires Zookeeper, so set that up first.

* Download zookeeper. Find a mirror at http://www.apache.org/dyn/closer.cgi/zookeeper/, pick a stable zookeeper, and download it to your server.

* Unpack zookeeper. For this guide we'll install it in `/opt`.

```
cd /opt
tar -zxvf /root/zookeeper-3.4.8.tar.gz # update path if you downloaded elsewhere.
ln -s /opt/zookeeper-3.4.8 /opt/zookeeper
mkdir /var/lib/zookeeper
```

* Make a config file for zookeeper:

```
cat << EOF > /opt/zookeeper/conf/zoo.cfg
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
EOF
```

* Start zookeeper: `/opt/zookeeper/bin/zkServer.sh start`

([more info](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html))

### Kafka

We recommend 0.10 or higher.

* Download kafka. Find a mirror at https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz, and download kafka to your server.

* Unpack kafka. Like zookeeper, we'll do so in `/opt`.

```
cd /opt
tar -zxvf /root/kafka_2.11-0.10.0.1.tgz  # update path if you downloaded elsewhere
ln -s /opt/kafka_2.11-0.10.0.1 /opt/kafka
```

* Start kafka: `/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties`

The log - if you need it - lives at /opt/kafka/logs/server.log

([more info](https://kafka.apache.org/documentation.html#quickstart))

## Configuration

Now edit the file at `/etc/raintank/metrictank.ini`.  It should be commented enough to guide you through the various options.

In particular, you'll probably want to change the following options:

* `statsd-addr`
* `cassandra-addrs`
* `kafka-mdm-in`: `brokers`, `enabled`
* `elasticsearch-idx`: `enabled`, `hosts`

Out of the box, one input is enabled: the [Carbon line input](https://github.com/raintank/metrictank/blob/master/docs/inputs.md#carbon)
It uses a default storage-schemas to coalesce every incoming metric into 1 second resolution.  You may want to fine tune this for your needs.
At `/etc/raintank/storage-schemas.conf`. (or simply what you already use in a pre-existing Graphite install).
See the input plugin documentation referenced above for more details.

If you want to use Kafka, you should enable the Kafka-mdm input plugin.  See [the Inputs docs for more details](https://github.com/raintank/metrictank/blob/master/docs/inputs.md).
See the `kafka-mdm-in` section in the config for the options you need to tweak.

## Run it!

```
systemctl start metrictank
```

Note that metrictank simply logs to stdout.
You can use something like `journalctl -u metrictank` to see the logs.

## Play with it!

In Grafana, you can now add a graphite datasource with url `http://<ip>:8080`.
If you access Grafana over https, make sure to use proxy mode, otherwise browsers will refuse to load content from the http datasource.

You can start visualizing the data that's already in there by importing
* [Metrictank dashboard](https://grafana.net/dashboards/279): visualizes all metrictank's internal performance metrics, which it sends via statsd/statsdaemon, into itself.  This dashboard will not work if you disabled statsd.
* [Statsdaemon dashboard](https://grafana.net/dashboards/297): if you use statsdaemon, you can visualize its performance metrics, stored in metrictank.

You're probably interested in loading in some fake data as well, perhaps to benchmark metrictank.
A full benchmarking guide is out of scope for this installation guide, but here are some suggestions:

* Use the [haggar](https://github.com/gorsuch/haggar) tool, which simulates independent clients, gradually appearing and sending data at randomized intervals into metrictank's carbon input port.  Invoke like so:

```
./haggar -agents 10 -jitter 1ms
```

* Use [fakemetrics](https://github.com/raintank/fakemetrics). which has a few modes of operation.  But one of the useful features is that it can send metrics in metrics2.0 format, into kafka.  You can do so using:

```
./fakemetrics -kafka-mdm-tcp-address localhost:9092 -orgs 100 -keys-per-org 1000
```

