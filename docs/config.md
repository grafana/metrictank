# Config

Metrictank comes with an [example main config file](https://github.com/raintank/metrictank/blob/master/metrictank-sample.ini),
a [storage-schemas.conf file](https://github.com/raintank/metrictank/blob/master/scripts/config/storage-schemas.conf) and
a [storage-aggregations.conf file](https://github.com/raintank/metrictank/blob/master/scripts/config/storage-aggregation.conf)

The files themselves are well documented, but for your convenience, they are replicated below.  

Config values for the main ini config file can also be set, or overridden via environment variables.
They require the 'MT_' prefix.  Any delimiter is represented as an underscore.
Settings within section names in the config just require you to prefix the section header.

Examples:

```
MT_LOG_LEVEL: 1                           # MT_<setting_name>
MT_CASSANDRA_WRITE_CONCURRENCY: 10        # MT_<setting_name>
MT_KAFKA_MDM_IN_DATA_DIR: /your/data/dir  # MT_<section_title>_<setting_name>
```

---


# Metrictank.ini


sample config for metrictank  
the defaults here match the default behavior.  
## misc ##

```
# instance identifier. must be unique. used in clustering messages, for naming queue consumers and emitted metrics.
instance = default
# accounting period to track per-org usage metrics
accounting-period = 5min
```

## data ##

```
# see https://github.com/raintank/metrictank/blob/master/docs/memory-server.md for more details
# max age for a chunk before to be considered stale and to be persisted to Cassandra
chunk-max-stale = 1h
# max age for a metric before to be considered stale and to be purged from in-memory ring buffer.
metric-max-stale = 6h
# Interval to run garbage collection job
gc-interval = 1h
# duration before secondary nodes start serving requests
# shorter warmup means metrictank will need to query cassandra more if it doesn't have requested data yet.
# in clusters, best to assure the primary has saved all the data that a newly warmup instance will need to query, to prevent gaps in charts
warm-up-period = 1h
```

## metric data storage in cassandra ##

```
# see https://github.com/raintank/metrictank/blob/master/docs/cassandra.md for more details
# comma-separated list of hostnames to connect to
cassandra-addrs =
# keyspace to use for storing the metric data table
cassandra-keyspace = metrictank
# desired write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one
cassandra-consistency = one
# how to select which hosts to query
# roundrobin                : iterate all hosts, spreading queries evenly.
# hostpool-simple           : basic pool that tracks which hosts are up and which are not.
# hostpool-epsilon-greedy   : prefer best hosts, but regularly try other hosts to stay on top of all hosts.
# tokenaware,roundrobin              : prefer host that has the needed data, fallback to roundrobin.
# tokenaware,hostpool-simple         : prefer host that has the needed data, fallback to hostpool-simple.
# tokenaware,hostpool-epsilon-greedy : prefer host that has the needed data, fallback to hostpool-epsilon-greedy.
cassandra-host-selection-policy = tokenaware,hostpool-epsilon-greedy
# cassandra timeout in milliseconds
cassandra-timeout = 1000
# max number of concurrent reads to cassandra
cassandra-read-concurrency = 20
# max number of concurrent writes to cassandra
cassandra-write-concurrency = 10
# max number of outstanding reads before blocking. value doesn't matter much
cassandra-read-queue-size = 100
# write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have
cassandra-write-queue-size = 100000
# how many times to retry a query before failing it
cassandra-retries = 0
# CQL protocol version. cassandra 3.x needs v3 or 4.
cql-protocol-version = 4
# enable SSL connection to cassandra
cassandra-ssl = false
# cassandra CA certficate path when using SSL
cassandra-ca-path = /etc/metrictank/ca.pem
# host (hostname and server cert) verification when using SSL
cassandra-host-verification = true
# enable cassandra user authentication
cassandra-auth = false
# username for authentication
cassandra-username = cassandra
# password for authentication
cassandra-password = cassandra
```

## Profiling and logging ##

```
# see https://golang.org/pkg/runtime/#SetBlockProfileRate
block-profile-rate = 0
# 0 to disable. 1 for max precision (expensive!) see https://golang.org/pkg/runtime/#pkg-variables")
mem-profile-rate = 524288 # 512*1024
# inspect status frequency. set to 0 to disable
proftrigger-freq = 60s
# path to store triggered profiles
proftrigger-path = /tmp
# minimum time between triggered profiles
proftrigger-min-diff = 1h
# if process consumes this many bytes (see bytes_sys in dashboard), trigger a heap profile for developer diagnosis
# set it higher than your typical memory usage, but lower than how much RAM the process can take before its get killed
proftrigger-heap-thresh = 25000000000
# only log log-level and higher. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL
log-level = 2
```

## Retention settings ##

```
[retention]
# path to storage-schemas.conf file
schemas-file = /etc/metrictank/storage-schemas.conf
# path to storage-aggregation.conf file
aggregations-file = /etc/metrictank/storage-aggregation.conf
```

## instrumentation stats ##

```
[stats]
# enable sending graphite messages for instrumentation
enabled = true
# stats prefix (will add trailing dot automatically if needed)
# The default matches what the Grafana dashboard expects
# $instance will be replaced with the `instance` setting.
# note, the 3rd word describes the environment you deployed in.
prefix = metrictank.stats.default.$instance
# graphite address
addr = localhost:2003
# interval at which to send statistics
interval = 1
# how many messages (holding all measurements from one interval. rule of thumb: a message is ~25kB) to buffer up in case graphite endpoint is unavailable.
# With the default of 20k you will use max about 500MB and bridge 5 hours of downtime when needed
buffer-size = 20000
```

## chunk cache ##

```
[chunk-cache]
# maximum size of chunk cache in bytes. (1024 ^ 3) * 4 = 4294967296 = 4G
max-size = 4294967296
```

## http api ##

```
[http]
# tcp address for metrictank to bind to for its HTTP interface
listen = :6060
# use gzip compression
gzip = true
# use HTTPS
ssl = false
# SSL certificate file
cert-file = /etc/ssl/certs/ssl-cert-snakeoil.pem
# SSL key file
key-file = /etc/ssl/private/ssl-cert-snakeoil.key
# limit on how many points could be requested in one request. 1M allows 500 series at a MaxDataPoints of 2000. (0 disables limit)
max-points-per-req = 1000000
# max number of data points to be returned per target
max-points-per-target = 86400
# limit on what kind of time range can be requested in one request. the default allows 500 series of 2 years. (0 disables limit)
max-days-per-req = 365000
# only log incoming requests if their timerange is at least this duration. Use 0 to disable
log-min-dur = 5min
```

## metric data inputs ##
### carbon input (optional)

```
[carbon-in]
enabled = false
# tcp address
addr = :2003
# represents the "partition" of your data if you decide to partition your data.
partition = 0
```

### kafka-mdm input (optional, recommended)

```
[kafka-mdm-in]
enabled = false
# tcp address (may be given multiple times as a comma-separated list)
brokers = kafka:9092
# kafka topic (may be given multiple times as a comma-separated list)
topics = mdm
# offset to start consuming from. Can be one of newest, oldest,last or a time duration
# the further back in time you go, the more old data you can load into metrictank, but the longer it takes to catch up to realtime data
offset = last
# kafka partitions to consume. use '*' or a comma separated list of id's
partitions = *
# save interval for offsets
offset-commit-interval = 5s
# directory to store partition offsets index. supports relative or absolute paths. empty means working dir.
# it will be created (incl parent dirs) if not existing.
data-dir =
# The number of metrics to buffer in internal and external channels
channel-buffer-size = 1000000
# The minimum number of message bytes to fetch in a request
consumer-fetch-min = 1
# The default number of message bytes to fetch in a request
consumer-fetch-default = 32768
# The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it
consumer-max-wait-time = 1s
#The maximum amount of time the consumer expects a message takes to process
consumer-max-processing-time = 1s
# How many outstanding requests a connection is allowed to have before sending on it blocks
net-max-open-requests = 100
```

## basic clustering settings ##

```
[cluster]
# The primary node writes data to cassandra. There should only be 1 primary node per shardGroup.
primary-node = true
# maximum priority before a node should be considered not-ready.
max-priority = 10
# the TCP/UDP address to listen on for the gossip protocol.
bind-addr = 0.0.0.0:7946
# TCP addresses of other nodes, comma separated. use this if you shard your data and want to query other instances.
# If no port is specified, it is assumed the other nodes are using the same port this node is listening on.
peers =
# Operating mode of cluster. (single|multi)
mode = single
# How long to wait before aborting http requests to cluster peers and returning a http 503 service unavailable
http-timeout = 60s
```

## clustering transports for tracking chunk saves between replicated instances ##
### kafka as transport for clustering messages (recommended)

```
[kafka-cluster]
enabled = false
# tcp address (may be given multiple times as a comma-separated list)
brokers = kafka:9092
# kafka topic (only one)
topic = metricpersist
# kafka partitions to consume. use '*' or a comma separated list of id's. Should match kafka-mdm-in's partitions.
partitions = *
# method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)
partition-scheme = bySeries
# offset to start consuming from. Can be one of newest, oldest,last or a time duration
offset = last
# save interval for offsets
offset-commit-interval = 5s
# Maximum time backlog processing can block during metrictank startup.
backlog-process-timeout = 60s
# directory to store partition offsets index. supports relative or absolute paths. empty means working dir.
# it will be created (incl parent dirs) if not existing.
data-dir =
```

### nsq as transport for clustering messages

```
[nsq-cluster]
enabled = false
# nsqd TCP address (may be given multiple times as comma-separated list)
nsqd-tcp-address =
# lookupd HTTP address (may be given multiple times as comma-separated list)
lookupd-http-address =
topic = metricpersist
channel = tank
# passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
producer-opt =
#passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
consumer-opt =
# max number of messages to allow in flight
max-in-flight = 200
```

## metric metadata index ##
### in memory, cassandra-backed

```
[cassandra-idx]
enabled = true
# Cassandra keyspace to store metricDefinitions in.
keyspace = metrictank
# comma separated list of cassandra addresses in host:port form
hosts = localhost:9042
#cql protocol version to use
protocol-version = 4
# write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one
consistency = one
# cassandra request timeout
timeout = 1s
# number of concurrent connections to cassandra
num-conns = 10
# Max number of metricDefs allowed to be unwritten to cassandra
write-queue-size = 100000
#automatically clear series from the index if they have not been seen for this much time.
max-stale = 0
#Interval at which the index should be checked for stale series.
prune-interval = 3h
# synchronize index changes to cassandra. not all your nodes need to do this.
update-cassandra-index = true
#frequency at which we should update flush changes to cassandra. only relevant if update-cassandra-index is true.
update-interval = 4h
# enable SSL connection to cassandra
ssl = false
# cassandra CA certficate path when using SSL
ca-path = /etc/metrictank/ca.pem
# host (hostname and server cert) verification when using SSL
host-verification = true
# enable cassandra user authentication
auth = false
# username for authentication
username = cassandra
# password for authentication
password = cassandra
```

### in-memory only

```
[memory-idx]
enabled = false
```

# storage-schemas.conf

```
# This config file sets up your retention rules.
# It is an extension of http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf
# Note:
# * You can have 0 to N sections
# * The first match wins, starting from the top. If no match found, we default to single archive of minutely points, retained for 7 days in 2h chunks
# * The patterns are unanchored regular expressions, add '^' or '$' to match the beginning or end of a pattern.
# * When running a cluster of metrictank instances, all instances should have the same agg-settings.
# * Unlike whisper (graphite), the config doesn't stick: if you restart metrictank with updated settings, then those
# will be applied. The configured rollups will be saved by primary nodes and served in responses if they are ready.
# (note in particular that if you remove archives here, we will no longer read from them)
# * Retentions must be specified in order of increasing interval and retention
# 
# A given rule is made up of 3 lines: the name, regex pattern and retentions.
# The retentions line can specify multiple retention definitions. You need one or more, space separated.
#
# There are 2 formats for a single retention definition:
# 1) 'series-interval:count-of-datapoints'                   legacy and not easy to read
# 2) 'series-interval:retention[:chunkspan:numchunks:ready]' more friendly format with optionally 3 extra fields
#
#Series intervals and retentions are specified using the following suffixes:
#
#s - second
#m - minute
#h - hour
#d - day
#y - year
#
# The final 3 fields are specific to metrictank and if unspecified, use sane defaults.
# See https://github.com/raintank/metrictank/blob/master/docs/memory-server.md for more details
#
# chunkspan: duration of chunks. e.g. 10min, 30min, 1h, 90min...
# must be valid value as described here https://github.com/raintank/metrictank/blob/master/docs/memory-server.md#valid-chunk-spans
# Defaults to a the smallest chunkspan that can hold at least 100 points.
#
# numchunks: number of raw chunks to keep in in-memory ring buffer
# See https://github.com/raintank/metrictank/blob/master/docs/memory-server.md for details and trade-offs, especially when compared to chunk-cache
# which may be a more effective method to cache data and alleviate workload for cassandra.
# Defaults to 2
#
# ready: whether the archive is ready for querying.  This is useful if you recently introduced a new archive, but it's still being populated
# so you rather query other archives, even if they don't have the retention to serve your queries
# Defaults to true
#
# Here's an example with multiple retentions:
# [apache_busyWorkers]
# pattern = ^servers\.www.*\.workers\.busyWorkers$
# retentions = 1s:1d:10min:1,1m:21d,15m:5y:2h:1:false
#
# This example has 3 retention definitions, the first and last override some default options (to use 10minutely and 2hourly chunks and only keep one of them in memory
# and the last rollup is marked as not ready yet for querying.

[default]
pattern = .*
retentions = 1s:35d:10min:7
```

# storage-aggregation.conf

```
# This config file controls which summaries are created (using which consolidation functions) for your lower-precision archives, as defined in storage-schemas.conf
# It is an extension of http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-aggregation-conf
# Note:
# * This file is optional. If it is not present, we will use avg for everything
# * Anything not matched also uses avg for everything
# * xFilesFactor is not honored yet.  What it is in graphite is a floating point number between 0 and 1 specifying what fraction of the previous retention level's slots must have non-null values in order to aggregate to a non-null value. The default is 0.5.
# * aggregationMethod specifies the functions used to aggregate values for the next retention level. Legal methods are avg/average, sum, min, max, and last. The default is average.
# Unlike Graphite, you can specify multiple, as it is often handy to have different summaries available depending on what analysis you need to do.
# When using multiple, the first one is used for reading.  In the future, we will add capabilities to select the different archives for reading.
# * the settings configured when metrictank starts are what is applied. So you can enable or disable archives by restarting metrictank.
#
# see https://github.com/raintank/metrictank/blob/master/docs/consolidation.md for related info.

[default]
pattern = .*
xFilesFactor = 0.1
aggregationMethod = avg,min,max
```

This file is generated by [config-to-doc](https://github.com/raintank/metrictank/blob/master/scripts/config-to-doc.sh)

