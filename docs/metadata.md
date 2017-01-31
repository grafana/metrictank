# Metadata index

Metrictank needs an index to efficiently lookup timeseries details by key or pattern.

Currently there are 2 index options. Only 1 index option can be enabled at a time.
* Memory-Idx
* Cassandra-Idx

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

This is the recommended option because it persists.

* type: Memory-Idx for search queries, backed by Cassandra for persistence
* persistence:  persists new metricDefinitions as they are seen.  At startup, the internal memory index is rebuilt from all metricDefinitions that have been stored in Cassandra.  Metrictank won’t be considered ready (be able to ingest metrics or handle searches) until the index has been completely rebuilt.
* efficiency: On low end hardware the index rebuilds at about 70000 metricDefinitions per second. Saving new metrics works pretty fast.

Metrictank will initialize Cassandra with the needed keyspace and tabe.  However if you are running a Cassandra cluster then you should tune the keyspace to suit your deployment.
Refer to the [cassandra guide](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md) for more details.

#### Configuration
```
[cassandra-idx]
enabled = false
# Cassandra keyspace to store metricDefinitions in.
keyspace = raintank
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

Note:
* All metrictanks write to Cassandra.  this is not very efficient.

## The anatomy of a metricdef

definition id's are unique across the entire system and can be computed from the def itself, so don't require coordination across distributed nodes.

there can be multiple definitions for each metric, if the interval changes for example

The schema is as follows:

```
type MetricDefinition struct {
	Id         string            
	OrgId      int               
	Name       string            // graphite format
	Metric     string            // kairosdb format (like graphite, but not including some tags)
	Interval   int               
	Unit       string            
	Mtype      string            
	Tags       []string          
	LastUpdate int64             
	Nodes      map[string]string 
	NodeCount  int               
}
```

See [the schema spec](https://github.com/raintank/schema/blob/master/metric.go#L78) for more details

## Developers' guide to index plugin writing
New indexes just need to implement the MetricIndex interface.

See [the Interface spec](https://github.com/raintank/metrictank/blob/master/idx/idx.go#L22) for more details
