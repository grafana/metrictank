# Metadata index

Metrictank needs an index to efficiently lookup timeseries details by key or pattern.

Currently there are 2 index options. Only 1 index options can be enabled at a time.
* Memory-Idx: 
* Elasticseach-Idx

### Elasticseach-Idx

The Elasticseach-Idx stores extends the Memory-Idx to provide persistent storage of the MetricDefinitions. At startup, the internal memory index is rebuilt from all metricDefinitions that have been stored in Elasticsearch.  Metrictank won’t be considered ready (be able to ingest metrics or handle searches) until the index has been completely rebuilt. The rebuild can take a few minutes if there are a large number of metricDefinitions stored in Elasticsearch.

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
# how often the retry buffer should be flushed to ES
retry-interval = 1h
# max number of concurrent connections to ES
max-conns = 20
# max number of docs to keep in the BulkIndexer buffer
max-buffer-docs = 1000
# max delay before the BulkIndexer flushes its buffer
buffer-delay-max = 10s
```

Note:
* All metrictanks write to ES.  this is not very efficient.  But we'll replace ES soon anyway.
* When indexing more than around ~ 50-100k metrics/s ES can start to block.  So if you're sending a large volume of (previously unseen)
  metrics all at once the indexing can [put backpressure on the ingestion](https://github.com/raintank/metrictank/blob/master/docs/operations.md#ingestion-stalls--backpressure), meaning
  less metrics/s while indexing is ongoing.
* Indexing to ES often tends to fail when doing many index operations at once.
  In this case we just reschedule to index the metricdef again within the next retry-interval.
  If you send a bunch of new data and the metrics are not showing up in ES yet, this is typically why.


### Memory-Idx

The Memory-Idx provides a high performant index store for searching.  The index uses about 1KB of memory per metricDefinition and can support 100's of 1000's of indexes per second and 10's of 1000's of searches per second on moderate hardware.  The memory-Idx is ephemeral. The index will be empty at startup and metrics will be indexed as they are received by metrictank.

#### Configuration
The memory-idx includes the following configuration section in the metrictank configuration file.

```
[memory-idx]
enabled = true
```

## The anatomy of a metricdef

definition id's are unique across the entire system and can be computed from the def itself, so don't require coordination across distributed nodes.

there can be multiple definitions for each metric, if the interval changes for example
currently those all just stored individually in the radix tree and trigram index, which is a bit redundant
in the future, we might just index the metric names and then have a separate structure to resolve a name to its multiple metricdefs, which could be cheaper.

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
