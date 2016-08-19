# Metadata index

Metrictank needs an index to efficiently lookup timeseries details by key or pattern.

Currently there are 2 index options. Only 1 index options can be enabled at a time.
* Memory-Idx: 
* Elasticseach-Idx

### Elasticseach-Idx

The Elasticseach-Idx stores extends the Memory-Idx to provide persistent storage of the MetricDefinitions. At startup, the internal memory index is rebuilt from all metricDefinitions that have been stored in cassandra.  Metrictank won’t be considered ready (be able to ingest metrics or handle searches) until the index has been completely rebuilt. The rebuild can take a few minutes if there are a large number of metricDefinitions stored in cassandra.

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
  In this case we just reschedule to index the metricdef again in between 30~60 minutes.
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
Currently the index is solely used for supporting Graphite style queries.  So, the index only needs to be able to search by a pattern that matches the MetricDefinition.Name field.
In future we plan to extend the searching capabilities to include the other fields in the definition.

Note:

* metrictank is a multi-tenant system where different orgs cannot see each other's data
* any given metric may appear multiple times, under different organisations

### Required query modes
An index plugin just needs to implement the github.com/raintank/metrictank/idx/MetricIndex interface.

```
type Node struct {
	Path string
	Leaf bool
	Defs []schema.MetricDefinition
}
type MetricIndex interface {
	Init(met.Backend) error
	Stop()
	Add(*schema.MetricData)
	Get(string) (schema.MetricDefinition, error)
	List(int) []schema.MetricDefinition
	Find(int, string) ([]Node, error)
	Delete(int, string) error
}

```

* Init(met.Backend): This is the initialization step performed at startup.  This method should block until the index is ready to handle searches.
* Stop(): This will be called when metrictank is shutting down.
* Add(*schema.MetricData):  Every metric received will result in a call to this method to ensure the metric has been added to the index.
* Get(string) (schema.MetricDefinition, error):  This method should return the MetricDefintion with the passed Id.
* List(int) []schema.MetricDefinition: This method should return all MetricDefinitions for the passed OrgId.  If the passed OrgId is "-1", then all metricDefinitions across all organisations should be returned.
* Find(int, string) ([]Node, error): This method provides searches.  The method is passed an OrgId and a query pattern. This pattern should be handled in the same way Graphite would. https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards.  Searches should return all nodes that match for the given OrgId and OrgId -1.
* Delete(int, string) error: This method is used for deleting items from the index. The method is passed an OrgId and a query pattern.  If the pattern matches a branch node, then all leaf nodes on that branch should also be deleted. So if the pattern is "*", all items in the index should be deleted.


