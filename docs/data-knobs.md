# Data knobs

See [the example config](https://github.com/raintank/metrictank/blob/master/metrictank-sample.ini) for an overview and basic explanation of what the config values are.
Some of the values related to chunking and compression are a bit harder to tune, so this article will explain in more detail.

## Compression tips

* values that never - or infrequently - change compress extremely well, so are very cheap to track and store.
* pay attention to your timestamps, make sure they are evenly spaced. That compresses better.
* storing values as integers (or more precisely: floats without decimal numbers) compresses very well.
  So you best store the numbers with the same unit as what your precision is.
  E.g. let's say you measure latencies such as 0.035 seconds (3 decimals precision, e.g. ms precision), it's better to
  track that as the number 35 (milliseconds) instead of 0.035 (seconds).

For more details, see the [go-tsz eval program](https://github.com/dgryski/go-tsz/tree/master/eval) or the 
[results table](https://raw.githubusercontent.com/dgryski/go-tsz/master/eval/eval-results.png)

## Chunk sizing and num chunks to keep in memory

### Basic guideline

`chunkspan` is how long of a timeframe should be covered by your chunks. E.g. you could store anywhere between 10 minutes to 24 hours worth of data in a chunk (chunks for each raw metric).
`numchunks` is simply how many chunks should be retained in RAM. (for each raw metric)

figuring out optimal configuration for the `chunkspan` and `numchunks` is not trivial.
The standard recommendation is 120 points per chunk and keep at least as much in RAM as what your commonly query for (+1 extra chunk, see below)

E.g. if your most common interval is 10s and most of your dashboards query for 2h worth of data, then the recommendation is:
```
chunkspan = 20min
numchunks = 7
```

20min because 120 points per chunk every 10 seconds is 1200 seconds or 20 minutes.
If you expected 6 chunks (20min * 6 = 2h), the answer is that you always need 1 extra chunk,
because the current chunk is typically incomplete and only covers a fraction of the ongoing 20min timeslot,
so you should always make sure to cover the requirements of one extra chunkspan.

Note:
* `chunkspan` and `numchunks` are currently global variables, which can't be finetuned on a per-metric or per-category level.
* when defining consolidation (rollups), you can specify custom chunkspans and numchunks for each rollup setting.  As rollups will have more time between points, it makes sense to choose longer chunkspans for rollups.

### Additional factors

Several factors come into play that may affect the above recommendation:

#### Rollups remove the need to keep large number of higher resolution chunks
If you roll-up data for archival storage, those chunks will also be in memory as per your configuration.
Querying for large timeframes may use the consolidated chunks in RAM, and keeping
extra raw (or higher-resolution) data in RAM becomes pointless, putting an upper bound on how many chunks to keep.  
See [Consolidation](https://github.com/raintank/metrictank/blob/master/docs/consolidation.md)


#### Compression efficiency

The more points are contained within a chunk, the more efficiently the compression can work. This is very noticeable
until about 120 points per chunk, at which point the improvement becomes less relevant.
For more details, see the [go-tsz eval program](https://github.com/dgryski/go-tsz/tree/master/eval) or the 
[results table](https://raw.githubusercontent.com/dgryski/go-tsz/master/eval/eval-results.png)


#### Garbage collection pressure

If garbage collection is a problem (which can be analyzed with the metrictank grafana dashboard), then this can be reduced by picking longer chunks and keeping fewer of them.
The more chunks you have, the more they need to be scanned by the Go garbage collector.
We plan to keep working on performance and memory management and hope to make this factor less and less relevant over time.

#### Memory overhead

Chunks come with a memory overhead (for the internal datastructure and bookkeeping).  Longer chunks with more data reduce the memory overhead.  Though this rarely seems to be a real concern.
Longer chunks however do force you to keep more data in RAM. Let's say you want to keep 1h worth of data in RAM. With a chunksize of 1h you need to keep 2 chunks in RAM 
(because the current chunk becomes empty at every start of a 1h interval), which in worst case leads up to 2h worth of data.  With a chunksize of 1min you need to keep 61 chunks in RAM,
worst case being 61 minutes.

#### Cassandra load

longer chunks with more points, means a lower rate of chunksaves, meaning fewer write commands and iops for cassandra.
There is probably an upper limit where cassandra becomes unhappy with large chunksizes, but we haven't seen that yet. 
Also, it may take longer for the write queue to drain with longer chunks.

#### Warm-up

The more data is needed in RAM, the more intense / longer the backfill or warm up is before a node can start serving requests.

#### Getting ready to become primary

longer chunk sizes means a longer backfill (with Kafka) (or a longer warm up with NSQ)

