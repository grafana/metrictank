# Metadata index

Metrictank needs an index to efficiently lookup timeseries details by key or pattern.

Currently there are 3 index options. Only 1 index option can be enabled at a time.
* Memory-Idx
* Elasticseach-Idx
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

This is the recommended option because it persists and is the fastet.

* type: Memory-Idx for search queries, backed by Cassandra for persistence
* persistence:  persists new metricDefinitions as they are seen.  At startup, the internal memory index is rebuilt from all metricDefinitions that have been stored in Cassandra.  Metrictank won’t be considered ready (be able to ingest metrics or handle searches) until the index has been completely rebuilt.
* efficiency: On low end hardware the index rebuilds at about 70000 metricDefinitions per second. Saving new metrics works pretty fast.

Metrictank will initialize Cassandra with the needed keyspace and tabe.  However if you are running a Cassandra cluster then you should tune the keyspace to suite your deployment.
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

### Elasticseach-Idx

* type: Memory-Idx for search queries, backed by Elasticsearch for persistence
* persistence: persists new MetricDefinitions as they are seen.  At startup, the internal memory index is rebuilt from all metricDefinitions that have been stored in Elasticsearch.  Metrictank won’t be considered ready (be able to ingest metrics or handle searches) until the index has been completely rebuilt.
* efficiency: The rebuild can take a few minutes if there are a large number of metricDefinitions stored in Elasticsearch.  Large volumes of new incoming metrics can overwhelm Elasticsearch.

This option is not recommended because it doesn't perform well and needs an additional dependency.  You should probably just use Cassandra-Idx.

Metrictank will initialize ES with the needed schema/mapping settings.

#### Configuration
The elasticsearch-idx includes the following configuration section in the metrictank configuration file.
```
[elasticsearch-idx]
enabled = false
# elasticsearch index name to use
index = metric
# Elasticsearch host addresses (multiple hosts can be specified as comma-separated list)
hosts = localhost:9200
# how often the retry buffer should be flushed to ES. Valid units are 's', 'm', 'h'.
retry-interval = 10m
# max number of concurrent connections to ES
max-conns = 20
# max number of docs to keep in the BulkIndexer buffer
max-buffer-docs = 1000
# max delay before the BulkIndexer flushes its buffer
buffer-delay-max = 10s
```

Note:
* All metrictanks write to ES.  this is not very efficient.  
* When indexing more than around ~ 50-100k metrics/s ES can start to block.  So if you're sending a large volume of (previously unseen)
  metrics all at once the indexing can [put backpressure on the ingestion](https://github.com/raintank/metrictank/blob/master/docs/operations.md#ingestion-stalls--backpressure), meaning
  less metrics/s while indexing is ongoing.
* Indexing to ES often tends to fail when doing many index operations at once.
  In this case we just reschedule to index the metricdef again within the next retry-interval.
  If you send a bunch of new data and the metrics are not showing up in ES yet, this is typically why.

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
