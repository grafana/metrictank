## ES
metric definitions are currently stored in ES as well as internally (other options can come later).
ES is the failsafe option used by graphite-raintank.py and such.
The index is used internally for the graphite-api and is experimental.  It's powered by a radix tree and trigram index.

note that any given metric may appear multiple times, under different organisations

definition id's are unique across the entire system and can be computed, so don't require coordination across distributed nodes.

there can be multiple definitions for each metric, if the interval changes for example
currently those all just stored individually in the radix tree and trigram index, which is a bit redundant
in the future, we might just index the metric names and then have a separate structure to resolve a name to its multiple metricdefs, which could be cheaper.

* We're also seeing ES blocking due to the metadata indexing around the 100k/s mark.  Both can and will be optimized more.


prefill at startup


## Developers' guide to index plugin writing

### required query modes
An index plugin needs to support:

* lookup (1) by id (used by graphite-metrictank. may be deprecated long term)
* lookup (1) by orgid (2) + target spec, where target spec is:
  - a graphite key
  - a graphite pattern that has wildcards (`*`), one of multiple options `{foo,bar}`, character lists `[abc]` and ranges `[a-z0-9]`.
  - in the future we will want to extend these with tag constraints (e.g. must have given key, key must have given value, or value for key must match a pattern similar to above pattern)
* prefix search of prefix pattern and orgid (2). this is a special case of a pattern search, but common for autocomplete/suggest with short prefix patterns.
* listing (e.g. graphite's metrics.json but possibly in more detail for other tools) based on org-id (2).


### notes

(1) lookup: What do we need to lookup? For now we mainly want/only need interval (for alignRequests), mtype (to figure out the consolidation) and name (for listings),
but ideally we can lookup the entire definition.  E.g. in the future we may end up determining rollup schema based on org and/or misc tags.
(2) org-id: we need to return metrics corresponding to a given org, as well as metrics from org -1, since those are publically visible to everyone.

### other requirements

* warm up a cold index (e.g. when starting an instance, needs to know which metrics are known to the system, as to serve requests early. actual timeseries data may be in ram or in cassandra)
