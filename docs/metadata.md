# Metadata index

Metrictank needs an index to efficiently lookup timeseries details by key or pattern.

Currently it's based on Elasticsearch, but we aim to provide alternatives soon.

### ES

metric definitions are currently stored in ES (for persistence) as well as internally (for faster lookups in some scenarios)
ES is queried by graphite-metrictank.py.

Metrictank will initialize ES with the needed schema/mapping settings. No configuration is needed.

Note:
* Metrictank will query ES at startup and backfill all definitions from ES into its internal index before it starts consumption.
* All metrictanks write to ES.  this is not very efficient.  But we'll replace ES soon anyway.
* When indexing more than around ~ 50-100k metrics/s ES can start to block.  So if you're sending a large volume of (previously unseen)
  metrics all at once the indexing can [put backpressure on the ingestion](https://github.com/raintank/metrictank/blob/master/docs/operations.md#ingestion-stalls--backpressure), meaning
  less metrics/s while indexing is ongoing.
* Indexing to ES often tends to fail when doing many index operations at once.
  In this case we just reschedule to index the metricdef again in between 30~60 minutes.
  If you send a bunch of new data and the metrics are not showing up yet, this is typically why.


### Internal index

Metrictank currently also has a built-in index, in the `idx` package,
which is used when querying metrictank directly (e.g. bypassing graphite)
and caching ES lookups.
It is also experimental and may be removed later.
It's powered by a radix tree and trigram index.

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

Note:

* metrictank is a multi-tenant system where different orgs cannot see each other's data
* any given metric may appear multiple times, under different organisations

### Required query modes
An index plugin needs to support:

* lookup (1) by id (used by graphite-metrictank. may be deprecated long term)
* lookup (1) by orgid (2) + target spec, where target spec is:
  - a graphite key
  - a graphite pattern that has wildcards (`*`), one of multiple options `{foo,bar}`, character lists `[abc]` and ranges `[a-z0-9]`.
  - in the future we will want to extend these with tag constraints (e.g. must have given key, key must have given value, or value for key must match a pattern similar to above pattern)
* prefix search of prefix pattern and orgid (2). this is a special case of a pattern search, but common for autocomplete/suggest with short prefix patterns.
* listing (e.g. graphite's metrics.json but possibly in more detail for other tools) based on org-id (2).
* in the future we may also do queries on tags such as:
  - list all tags
  - list all tags for a given series pattern


### Notes

(1) lookup: What do we need to lookup? For now we mainly want/only need interval (for alignRequests), mtype (to figure out the consolidation) and name (for listings),
but ideally we can lookup the entire definition.  E.g. in the future we may end up determining rollup schema based on org and/or misc tags.  
(2) org-id: we need to return metrics corresponding to a given org, as well as metrics from org -1, since those are publically visible to everyone.

### Other requirements

* warm up a cold index (e.g. when starting an instance, needs to know which metrics are known to the system, as to serve requests early. actual timeseries data may be in ram or in cassandra)
