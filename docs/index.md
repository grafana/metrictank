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
