# Metadata index

Grafana Metrictank needs an index to efficiently lookup timeseries details by key or pattern.

Currently there are 3 index options. Only 1 index option can be enabled at a time.
* Memory-Idx
* Cassandra-Idx
* Bigtable-Idx

### Memory-Idx

* type: in-process in memory
* persistence: none.  index will be empty at every start of the process. Metrics are indexed as they are received by metrictank.
* efficiency: about 1KB of memory per metricDefinition.  Supports 100's of 1000's of indexes per second and 10's of 1000's of searches per second on moderate hardware.

#### Configuration
The memory-idx includes the following configuration section in the metrictank configuration file.

```
[memory-idx]
enabled = true
```

### Cassandra-Idx

* type: Memory-Idx for search queries, backed by Cassandra for persistence
* persistence:  persists new metricDefinitions as they are seen and every update-interval.  At startup, the internal memory index is rebuilt from all metricDefinitions that have been stored in Cassandra.  Grafana Metrictank won’t be considered ready (be able to ingest metrics or handle searches) until the index has been completely rebuilt.
* efficiency: On low end hardware the index rebuilds at about 70000 metricDefinitions per second. Saving new metrics works pretty fast.

Grafana Metrictank will initialize Cassandra with the needed keyspace and tabe.  However if you are running a Cassandra cluster then you should tune the keyspace to suit your deployment.
Refer to the [cassandra guide](https://github.com/grafana/metrictank/blob/master/docs/cassandra.md) for more details.

#### Configuration
```
[cassandra-idx]
enabled = false
# Cassandra keyspace to store metricDefinitions in.
keyspace = raintank
# Cassandra table to store metricDefinitions in.
table = metric_idx
# Cassandra table to archive metricDefinitions in.
archive-table = metric_idx_archive
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
```

### Bigtable-Idx

Similar to the cassandra idx, but uses bigtable.



## The anatomy of a metricdef

definition id's are unique across the entire system and can be computed from the def itself, so don't require coordination across distributed nodes.

there can be multiple definitions for each metric, if the interval changes for example

The schema is as follows:

```
type MetricDefinition struct {
	Id         string
	OrgId      int
	Name       string            // graphite format
	Interval   int
	Unit       string
	Mtype      string
	Tags       []string
	LastUpdate int64
	Partition  int
}
```

Typically the MetricDefinition is wrapped in a structure that contains more information (but which is not persisted), like so:
```
type Archive struct {
        schema.MetricDefinition
        SchemaId uint16 // index in mdata.schemas (not persisted)
        AggId    uint16 // index in mdata.aggregations (not persisted)
        IrId     uint16 // index in mdata.indexrules (not persisted)
        LastSave uint32 // last time the metricDefinition was saved to a persistent index
}
```

See the source code for [MetricDefinition](https://github.com/grafana/metrictank/blob/master/schema/metric.go) and
[MetricData](https://github.com/grafana/metrictank/blob/master/idx/idx.go) for more information.

### LastSave vs LastUpdate

A common source of confusion are the `LastUpdate`, `LastSave` and `lastWrite` fields.
The first two are concepts in the index, whereas the latter is in the tank (data chunks).

#### LastUpdate

`LastUpdate` reflects a data timestamp. It is the highest timestamp seen in the data stream for the metric.

Note:
* it is independent of the wallclock: we only look at the timestamps in the data stream, not at the time when we received the data
* if messages come in out of order - with older data timestamps - we stick with the highest timestamp seen.

Used for:
* filtering query search results (based on the `from` of the query).
* pruning: cutoff times derived from index-rules.conf are compared against LastUpdate.
  (this means that with index pruning set at 7days, if the data stream lags behind by 8 days (e.g. we just received a point for 8 days ago), the series
  would still be pruned. But typically this is not an issue since data streams should have much less lag than the configured pruning cutoff).

#### LastSave

`LastSave` is the wallclock time of when we last issued a save of the MetricDefinition to the persistent index
The main use is determining when a MetricDefinition should be persisted. As it would be unpractical to persist it
every single time there is an update in the index (e.g. `LastUpdate` tends to change with every single message received)
Persist index plugins like Cassandra-Idx and Bigatble-Idx allow for a configurable timeframe (`update-interval`) of "out of dateness"..

Note:
* it is independent of the data timestamps. It is only based on the current wallclock time of when we issue the save operation.
* When we issue the save, the MetricDefinition will get queued before it actually gets saved. But typically the queueing time is insignificant.
* When loading the index at startup, we base the `LastSave` off of `LastUpdate`. This is technically incorrect but in practice doesn't cause issues.


#### lastWrite

`lastWrite` is the wall clock time of when last point was successfully added to an AggMetric in the tank (possibly to its reorder buffer).
It is independent of the data timestamps.

Used for AggMetric garbage collection (determining when to seal chunks and flush them, and possibly removing data from the tank)


## Developers' guide to index plugin writing

New indexes just need to implement the MetricIndex interface.

See [the Interface spec](https://github.com/grafana/metrictank/blob/master/idx/idx.go#L22) for more details
