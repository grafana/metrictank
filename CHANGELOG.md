# master

## breaking changes

* as of v0.13.1-186-gc75005d the `/tags/delSeries` no longer accepts a `propagate` parameter.
  It is no longer possible to send the request to only a single node, it now always propagates to all nodes, bringing this method in line with `/metrics/delete`.
* as of v0.13.1-38-gb88c3b84 by default we reject data points with a timestamp far in the future.
  By default the cutoff is at 10% of the raw retention's TTL, so for example with the default
  storage schema `1s:35d:10min:7` the cutoff is at `35d*0.1=3.5d`. 
  The limit can be configured by using the parameter `retention.future-tolerance-ratio`, or it can
  be completely disabled by using the parameter `retention.enforce-future-tolerance`. 
  To predict whether Metrictank would drop incoming data points once the enforcement is turned on,
  the metric `tank.sample-too-far-ahead` can be used, this metric counts the data points which
  would be dropped if the enforcement were turned on while it is off.
  #1572 
* Prometheus integration removal. As of v0.13.1-97-gd77c5a31, it is no longer possible to use metrictank
  to scrape prometheus data, or query data via Promql.  There was not enough usage (or customer interest)
  to keep maintaining this functionality.
  #1613
* as of v0.13.1-110-g6b6f475a tag support is enabled by default, it can still be disabled though.
  This means if previously metrics with tags have been ingested while tag support was disabled,
  then those tags would have been treated as a normal part of the metric name, when tag support
  now gets enabled due to this change then the tags would be treated as tags and they wouldn't
  be part of the metric name anymore. As a result there is a very unlikely scenario in which some
  queries don't return the same results as before, if they query for tags as part of the metric
  name.
  #1619
* as of v0.13.1-250-g21d1dcd1 metrictank no longer excessively aligns all data to the same
  lowest comon multiple resolution, but rather keeps data at their native resolution when possible.
  1. When queries request mixed resolution data, this will now typically result in larger response datasets,
  with more points, and thus slower responses.
  Though the max-points-per-req-soft and max-points-per-req-hard settings will still help curb this problem.
  Note that the hard limit was previously not always applied correctly.
  Queries may run into this limit (and error) when they did not before.
  2. This version introduces 2 new optimizations (see pre-normalization and mdp-optimization settings).
  The latter is experimental and disabled by default, but the former is recommended and enabled by default.
  It helps with alleviating the extra cost of queries in certain cases
  (See https://github.com/grafana/metrictank/blob/master/docs/render-path.md#pre-normalization for more details)
  When upgrading a cluster in which you want to enable pre-normalization (recommended),
  you must apply caution: pre-normalization requires a PNGroup property to be
  communicated in intra-cluster data requests, which older peers don't have.
  The peer receiving the client request, which fans out the query across the cluster, will only set
  the flag if the optimization is enabled (and applicable).  If the flag is set for the requests,
  it will need the same flag set in the responses it receives from its peers in order to tie the data back to the initiating requests. 
  Otherwise, the data won't be included in the response, which may result in missing series, incorrect aggregates, etc.
  Peers responding to a getdata request will include the field in the response, whether it has the
  optimization enabled or not.

  Thus, to upgrade an existing cluster, you have 2 options:
  A) disable pre-normalization, do an in-place upgrade. enable it, do another in-place upgrade.
     This works regardless of whether you have a separate query peers, and regardless of whether you first
     upgrade query or shard nodes.
  B) do a colored deployment: create a new gossip cluster that has the optimization enabled from the get-go,
     then delete the older deployment.

## other

* dashboard tweaks. #1557, #1618
* performance improvement meta tags #1541, #1542
* docs improvements #1559 , #1620
* bigtable index fix: only load current metricdefs. #1564
* Fix deadlock when write queue full. #1569
* tags/findSeries - add lastts-json format. #1580
* add catastrophe recovery for cassandra (re-resolve when all IP's have changed). #1579
* mt-whisper-importer-reader: print message when everything done with the final stats. #1617
* Add `/tags/terms` query to get counts of tag values #1582 
* add function offset() #1621
* expr: be more lenient: allow quoted ints and floats #1622
  
# v0.13.1: Meta tag and http api improvements, lineage metadata, per partition metrics and more. Nov 28, 2019.

## meta tags

* correctly clear enrichment cache on upsert #1472
* meta tag records must be optional in meta tag upsert requests #1473
* Persist meta records to Cassandra index #1471
* Remove hashing in enricher #1512
* skip meta tag enrichment when we can #1515
* Optimize autocomplete queries #1514
* Performance optimizations for meta tag queries #1517
* Rewrite enricher to use map lookup #1523

## reorder buffer

* version v0.13.0-188-g6cd12d6 introduces storage-schemas.conf option 'reorderBufferAllowUpdate' to allow for some data to arrive out of order. #1531

## http api

* Added "ArgQuotelessString" argument for interpreting strings in the request that don't have quotes around tem (e.g. keepLastValue INF)
* Fix /find empty response resulting in "null" #1464
* patch and update macaron/binding middleware to support content-length header for GET requests #1466
* Fix removeAboveBelowPercentile panic #1518
* rollup indicator (lineage information in response metadata) #1481, #1508
* return proper errors upon bad request from user #1520
* correct for delayed lastUpdate updates #1532

## monitoring

* report the priority and readiness per partition as a metric #1504, #1507
* dashboard fix: maxSeries not a valid groupByNodes callback. #1491
* MemoryReporter: make call to runtime.ReadMemStats time bound to avoid lost metrics #1494

## tools

* remove deprecated cli argument ttls from mt-whisper-importer-writer
* add tool to calculate the id of metrics: mt-keygen #1526

## misc

* Don't validate MaxChunkSpan if BigTable store is disabled #1470
* lower default max chunk cache size to 512MB #1476
* add initial hosted metrics graphite documentation #1501, #1502
* Add 'benchmark' target to Makefile that runs all benchmarks #1498
* in cluster calls, set user agent #1469

## docker stack

* cleanup docker img versions #1479 
* remove metrictank bits from graphite-storage-schemas.conf files #1553

# v0.13.0: Meta tags beta, sharding by tags, new importer (bigtable!), response stats, memory-idx write queue and many fixes. Sept 17, 2019.

## breaking changes

* as of v0.13.0-160-gd2703083 the default setting for `memory-idx.tag-query-workers` is `5` instead of `50`.
  If a user still has the value `50` in their config file we recommend decreasing that, because due to how
  meta tag queries get processed MT may now create multiple pools of workers concurrently to process a single
  query, where each pool consists of `tag-query-workers` threads.
* as of v0.13.0-75-geaac736a Metrictank requires two new Cassandra tables if the meta tag feature is enabled and the Cassandra index is used. It only creates them automatically if `cassandra-idx-create-keyspace` is set to true.
* as of v0.12.0-404-gc7715cb2 we clean up poorly formatted graphite metrics better. To the extent that they have previously worked, queries may need some adjusting
  #1435
* version v0.12.0-96-g998933c3 introduces config options for the cassandra/scylladb index table names.
  The default settings and schemas match the previous behavior, but people who have customized the schema-idx template files
  should know that we now no longer only expand the keyspace (and assume a hardcoded table name).
  Now both the `schema_table` and `schema_archive_table` sections in the template name should have 2 `%s` sections which will be
  expanded to the `keyspace` and `table`, or `keyspace` and `archive-table` settings respectively configured under `cassandra-idx` of the metrictank config file.
* version v0.12.0-81-g4ee87166 and later reject metrics with invalid tags on ingest by default, this can be disabled via the `input.reject-invalid-tags` flag.
  if you're unsure whether you're currently sending invalid tags, it's a good idea to first disable the invalid tag rejection and watch the
  new counter called `input.<input name>.metricdata.discarded.invalid_tag`, if invalid tags get ingested this counter will increase without
  rejecting them. once you're sure that you don't ingest invalid tags you can enable rejection to enforce the validation.
  more information on #1348
* version v0.12.0-54-g6af26a3d and later have a refactored jaeger configuration + many more options #1341
  the following config options have been renamed:
  - `tracing-enabled -> jaeger.enabled`
  - `tracing-addr -> jaeger.agent-addr`
  - `tracing-add-tags -> jaeger.add-tags` (now also key=value instead of key:value)
* as of v0.12.0-43-g47bd3cb7 mt-whisper-importer-writer defaults to the new importer path, "/metrics/import" instead of "/chunks" and
  uses a "http-endpoint" flag instead of "listen-address" and "listen-port".

## importer

* bigtable importer #1291
* Make the importer utilities rely on TSDB-GW for authentication and org-association #1335
* fix TTL bug: calculate TTL relative to now when inserting into cassandra. #1448

## other

* meta tags (beta feature): 
  - Implement series lookup and filtering by meta tag #1423 
  - Meta tag enrichment #1433
  - Auto complete with meta tags #1438
  - Batch update meta records #1442
  Left to do: rule persistence.
* fix kafka backlog processing to not get stuck/timeout if no messages #1315, #1328, #1350, #1352, #1360
* memleak fix: Prevent blocked go routines to hang forever #1333, #1337
* update jaeger client v2.15.0 -> v2.16.0, jaeger-lib v1.2.1 -> v2.0.0 #1339
* Update Shopify/sarama from v1.19.0 to v1.23.0
* add orgid as jaeger tag, to ease searching by orgid #1366
* Fix active series stats decrement #1336
* render response metadata: stats #1334
* fix prometheus input plugin resetting its config at startup #1346
* make index/archive tables configurable #1348
* add writeQueue buffer to memoryIdx #1365
* remove tilde from name values when indexing tags #1371
* Jaeger cleanup: much fewer spans, but with more stats - and more stats for meta section #1380, #1410
* increase connection pool usage #1412
* New flag 'ingest-from' #1382
* flush aggregates more eagerly when we can #1425
* Peer query speculative fixes and improvements #1430
* support sharding by tags #1427, #1436, #1444
* Fix uneven length panics #1452

## new query api functions

* fallbackSeries #1326
* group #1324
* integral #1325

# v0.12.0: Query nodes, find cache and various performance tweaks. May 14, 2019

## Important changes that require your attention:

* This release includes the "query layer" functionality.
Versions prior to v0.11.0-184-g293b55b9 cannot handle query nodes joining the cluster and will crash.
To deploy the new query nodes and introduce them into the cluster, you must first
upgrade all other nodes to this version (or later)
Also, regarding cluster.mode:
    - 'single' is now 'dev'
    - 'multi' is now 'shard'
(The old values are still allowed for the time being)
see #1243, #1292, #1295

* since v0.11.0-169-g59ebb227, kafka-version now defaults to 2.0.0 instead of 0.10.0.0. Make sure to set
  this to a proper version if your brokers are not at least at version 2.0.0.
  See #1221

* since v0.11.0-233-gcf24c43a, if queries need rollup data, but asked for a consolidateBy() without matching rollup aggregation
  we pick the most approriate rollup from what is available.

* since v0.11.0-252-ga1e41192, remove log-min-dur flag, it was no longer used. #1275

* Since v0.11.0-285-g4c862d8c, duplicate points are now always rejected, even with the reorder buffer enabled.
  Note from the future: this was undone in v0.13.0-188-g6cd12d6, see future notes about reorderBufferAllowUpdate

## index

* cassandra index: load partitions in parallel. #1270
* Add partitioned index (experimental and not recommended) #1232
* add the mt-index-prune utility #1231, #1235
* fix index corruption: make sure defBy{Id,TagSet} use same pointers #1303

## api

* Improve performance of SetTags #1158
* speed up cross series aggregators by being more cache friendly #1164
* Fix summarize crash #1170
* groupByTags Performance improvements + fix setting consolidator per group + fix alias functions name tag setting #1165
* Meta tags part 1: meta record data structures and corresponding CRUD api calls (experimental) #1301
* Add absolute function #1300
* Add find cache to speed up render requests #1233, #1236, #1263, #1265, #1266, #1285
* Added 8 functions related to filterSeries #1308
* Added cumulative function #1309

## docs

* document startup procedure #1186
* Better document priority ready state #1223

## monitoring

* fix render latency queries + remove old dashboards #1192
* Dashboard: mem.to_iter fix and use UTC #1219
* refactor ingestion related metrics, in particular wrt drops. add prometheus stats.  #1278, #1288
* fix decrement tank.total_points upon GC. fix #1239

## Misc

* Dynamic GOGC based on ready state #1194
* improve kafka-mdm stats/priority tracking #1200
* tweak cluster priority calculation to be resilient against GC pauses #1022, #1218
* update messagepack to v1.1 #1214
* mt-index-cat should use NameWithTags() when listing series #1267
* improvement for reorder buffer. #1211
* debug builds and images. #1187

# v0.11.0 Bigtable, chunk formats, fixes and breaking changes. Dec 19, 2018

### Important changes that require your attention:

1) with our previous chunk format, when both:
   - using chunks of >4 hours
   - the time delta between start of chunk and first point is >4.5 hours

   the encoded delta became corrupted and reading the chunk results in incorrect data.
   This release brings a remediation to recover the data at read time, as well
   as a [new chunk format](https://github.com/grafana/metrictank/blob/master/devdocs/chunk-format.md#tszserieslong) that does not suffer from the issue.
   The new chunks are also about 9 bytes shorter in the typical case.
   While metrictank now writes to the store exclusively using the new format, it can read from the store in any of the formats.
   This means readers should be upgraded before writers,
   to avoid the situation where an old reader cannot parse the chunk written by a newer
   writer during an upgrade.  See #1126, #1129
2) we now use logrus for logging #1056, #1083
   Log levels are now strings, not integers.
   See the [updated config file](https://github.com/grafana/metrictank/blob/master/docs/config.md#profiling-and-logging)
3) index pruning is now configurable via [index-rules.conf](https://github.com/grafana/metrictank/blob/master/docs/config.md#index-rulesconf) #924, #1120
   We no longer use a `max-stale` setting in the `cassandra-idx` section,
   and instead gained an `index-rules-conf` setting.
4) The NSQ cluster notifier has been removed. NSQ is a delight to work with, but we could
   only use it for a small portion of our clustering needs, requiring Kafka anyway for data ingestion
   and distribution. We've been using Kafka for years and neglected the NSQ notifier code, so it's time to rip it out.
   See #1161
5) the offset manager for the kafka input / notifier plugin has been removed since there was no need for it.
   `offset=last` is thus no longer valid. See #1110 

# index and store
* support for bigtable index and storage #1082, #1114, #1121
* index pruning rate limiting #1065 , #1088 
* clusterByFind: limit series and streaming processing #1021 
* idx: better log msg formatting, include more info #1119

# clustering
* fix nodes sometimes not becoming ready by dropping node updates that are old or about thisNode. #948

# operations
* disable tracing for healthchecks #1054 
* Expose AdvertiseAddr from the clustering configuration #1063 , #1097 
* set sarama client KafkaVersion via config #1103 
* Add cache overhead accounting #1090, #1184
* document cache delete #1122
* support per-org `metrics_active` for scraping by prometheus #1160
* fix idx active metrics setting #1169
* dashboard: give rows proper names #1184

# tank
* cleanup GC related code #1166
* aggregated chunk GC fix (for sparse data, aggregated chunks were GC'd too late, which may result in data loss when doing cluster restarts),
  also lower default `metric-max-stale` #1175, #1176
* allow specifying timestamps to mark archives being ready more granularly #1178

## tools
* mt-index-cat: add partition support #1068 , #1085 
* mt-index-cat: add `min-stale` option, rename `max-age` to `max-stale` #1064
* mt-index-cat: support custom patterns and improve bench-realistic-workload-single-tenant.sh #1042 
* mt-index-cat: make `NameWithTags()` callable from template format #1157
* mt-store-cat: print t0 of chunks #1142
* mt-store-cat: improvements: glob filter, chunk-csv output #1147
* mt-update-ttl: tweak default concurrency, stats fix, properly use logrus #1167
* mt-update-ttl: use standard store, specify TTL's not tables, auto-create tables + misc #1173
* add mt-kafka-persist-sniff tool #1161
* fixes #1124

# misc
* better benchmark scripts #1015 
* better documentation for our input formats #1071 
* input: prevent integer values overflowing our index datatypes, which fixes index saves blocking #1143
* fix ccache memory leak #1078 
* update jaeger-client to 2.15.0 #1093 
* upgrade Sarama to v1.19, adding compatibility with Kafka v2.0 #1127
* fix panic caused by multiple closes of pluginFatal channel #1107
* correctly return error from NewCassandraStore() #1111
* clean way of skipping expensive and integration tests. #1155, #1156
* fix duration vars processing and error handling in cass idx #1141
* update release process, tagging, repo layout and version formatting. update to go1.11.4 #1177, #1180, #1181
* update docs for bigtable, storage-schemas.conf and tank GC #1182

# v0.10.1. performance fix: pruning effect on latency, go 1.11, etc.  Sep 24, 2018

* when pruning index, use more granular locking (prune individual metrics separately rather then all at once). this can significantly reduce request latencies, as previously, some render requests could be blocked a long time by long running index prunes (which was especially worse with high series churn). now there is practically no latency overhead (though prunes now run longer but that's not a problem) #1062
* emit the current MT version as a metric #1041 
* k8s: allow passing `GO*` variables as `MT_GO*` #1044
* better docs for running MT via docker #1040, #1047 
* make fragile duration integer config values parseable as string durations #1017 
* go 1.11 #1045 
* no need for `$(go list ./... | grep -v /vendor/)` #1050

# v0.10.0. Clustering important bugfix + faster ingest, speculative query execution, more graphite functions and lots more. Sep 11, 2018

There was a bug in 0.9 which caused instances to incorrectly encode Id's for tracking saved rollup chunks, which in some cases could cause data loss when write nodes restarted and would overwrite rollup chunks with partial chunks. Because of this, we strongly recommend upgrading to this version.

### index
* use atomics in index ingestion path, yielding about a ~30% ingestion speed improvement. dbd744084f06118f14ab17ea9ce1ac95ca42fd57, #945 
* Fix multi-interval series issue, handle regressing timestamps #897 
* fix race condition in Findtags #946 

### store and chunk cache 
* support cosmosdb, cassandra connectTimeout #922 
* refactor cassandrastore read queries (reduces cassandra CPU usage), support disabling chunkcache #931 
* chunk cache: Block to submit accounting events, add some metrics #1010 
* workaround chunk cache corruption bug 10a745c923a79f902f674625ea3f4f3b934a22fe 
* chunk cache perf fixes: AddRange + batched accounting #943 #1006
* chunk cache corruption testing #1009 

### core
* cleanup from-to range error handling in dataprocessor, aggmetrics, cache, store + fixes #919
* read from first chunks #994 
* Speculative query execution #956 #979 #1000 
* fix clustering chunk ID referencing. #972
* fix logger #991 
* set clustering (notifier) partitions more directly #952

### API server
* new functions: isNonNull #959 , scaleToSeconds #970 , countSeries #974 , filterSeries #980 , removeAboveBelowValue 34febb0bc243ab68ea1bc17478cb40629d0cb133, highest/lowest a958b51c37222a5f8d06412aafeaa497fcaa1756, asPercent #966 , derivative and nonNegativeDerivative #996 , sortBy, sortByMaxima, and sortByTotal #997 , removeAbovePercentile and removeBelowPercentile #992, keepLastValue #995  
* Fix summarize function #928 
* Add show plan endpoint #961 
* update gziper, saving memory allocations #964 
* workaround invalid HTTP status codes #987 
* endpoint for analyzing priority calculation #932 

### stats 
* handle write errors/timeouts and remote connection close better #918 
* Fix points return measurement overflow #953 
* fix pointsReturn to consider MaxDataPoints based consolidation #957 
* Monitor rss, vsz, cpu usage and page faults #1028 
* expose a few key metrics as rates as well #920

### build, docker environments & CI

* move metrictank.go to cmd directory #935, #939 
* vendoring #934
* fix dep, don't use gopkg.in for schema #1004 
* go 1.10.3, qa tests #990
* remove bogus test flags #1005 
* grafana/grafana:latest in all docker envs #1029 
* better qa scripts #1034 
* docker benchmark updates #1037 
* Docker grafana automatic provisioning #1039 

### Docs
* document graphite functions better #998 
* cassandra, devdocs #1003 
* update docker quickstart guide #1038 
* dev docs #1035 
* Doc updates #927 

### Tools
* suffix filter for mt-index-cat #1012 
* add mt-store-cp-experimental tool #1033 
* kafka-mdm-sniff: mode to print only invalid data #921 

# v0.9.0. major kafka ingestion format changes + some other stuff. May 16, 2018

## kafka format upgrade

support for new MetricPoint optimized data format in the kafka mdm topic, resulting in less kafka io, disk usage, GC workload, metrictank and kafka cpu usage, faster backfills. #876 , #885 #890, #891, #894, #911 
this also comes with:
* new dashboard
* updates of internal representations of keys, archives, index structures, intra-cluster requests, etc.(**so you must do a colored upgrade or new cluster deployment**)
* metrics changes for metrics_received, metrics_invalid changed see 01dabf9804971ebc8afb0d7186d4894a5e217c66, a772c1045a06a7542b0f1d76686037fda5624fee
* removal of mt-replicator-via-tsdb tool
* deprecation of .Metric field in MetricData and MetricDefinition. it is now ignored in incoming MetricData and in the cassandra index tables.
* mt-schemas-explain: also show built-in default
* various update to some of the docker stacks (use latest graphite, control x-org-id authentication, use prometheus docker monitoring, update for latest fakemetrics and tsdb-gw, etc)

## other
* upgrade to latest sarama (kafka) library. #905 
* remove pressure.idx and pressure.tank metrics, too much overhead and too little use. #905 
* sarama reduce default kafka-mdm channel-buffer-size #886 
* refactor chaos testing and carbon end2end test into a unified testing library and test functions. #830 
* migrate "public org id" functionality from magic org=-1 to a >0 orgid to be specified via `public-org` config setting. #880, #883 
* fix cluster load balancing #884
* initialize cassandra schemas via template files, support scylladb schemas. #898 
* support adding arbitrary extra tags in jaeger traces #904 
* Accept bool strings for ArgBool #895 

# v0.8.3. critical fix. March 16, 2018

* fix critical bug in 0.8.2 release #871
* use nameWithTags to match schema by pattern #872 

# v0.8.2. don't use. use 0.8.3 instead. Mar 15, 2018

mostly bugfixes + promql, auto kafka offset, ...
* fix tagdb routes #846 
* move backend stores into their own package #851 
* fix sortByName calls in dashboard 8ac5a28d2659e44a61a22644d6cafa716f06b2fd
* Initial tag query performance improvements #848 
* offset 'auto' option to set kafka offset to retention - segmentSize in k8s, with fallback to oldest #850 , #861 
* mt-update-ttl multithreaded (much faster) + more #843 
* Added missing batch/agg functions #847 
* experimental promql support #858
* Make sure rollup data is persisted before GC #840 
* Add support for graphite's `summarize` #837 
* fix: GC task too eagerly closes chunks. #844

# 0.8.1. bugfixes. Feb 1, 2018

## index
* split up tags and tree index internally #806
* add clauses to detect nil Node's in idx.Find #812 

## build
* split up circleCI jobs into parallel workflows #810 
* build metrictank-gcr docker image automatically in MT repo + trigger extra qa checks in separate environment #831 
* mt binaries should be in /usr/bin not /usr/sbin #832 

## new graphite functions
* diff, diffSeries, multiply, multiplySeries, rangeOf, rangeOfSeries, stddev, stddevSeries #824 
* exclude, grep #826 
* sortByName  #827 
* divideSeriesLists #833 

## bugfixes
* initialize logger properly, so you can see config error if that aborts startup. #825 
* fix input (particularly kafka-mdm) exit flow #748 
* Fix broken consolidation #836 
* Pass error back instead of crashing when non-configured, incorrect consolidation function used or unknown aggSpan in GetAggregated(). #839 
* consistent version string generation 99cf72124bb79f06a3f4b141dc7e7434bc3714bc
* make logging work for upstart 0.6.5

# v0.8.0: Happy 2018! Jan 30, 2018

# breaking changes

## tag index
MT now has an experimental tag-index built-in (compatible with graphite and we also aim to integrate with prometheus). This comes with an internal schema update. see #729 #749 , #750 , #755 , #759 , #762 , #774 , #779 
for this we introduced two new config flags:

* `tag-support` : whether to enable tag queries
* `match-cache-size` : internal, can be mostly ignored.

note : **tags in MetricDefinitions and MetricData are now validated for correctness** irrespective of tag-support configuration. for invalid incoming metrics, you will see `in: Invalid metric` debug log messages and incrementing of `input.*.metric_invalid` metrics. they are no longer ingested.

**metric names are now extended with tags, in the memory index, and in all query api output, potentially breaking dashboards (not in the persisted index)**

## cassandra
### omit read requests when they are too old and when the read queue is full
(instead of the previous blocking behavior) #685 
config:
* `cassandra-read-queue-size` now defaults to 200000 instead of 100. **update your value otherwise you may see read requests dropped too eagerly**
* `cassandra-omit-read-timeout` (new) setting defaults to 60s

new metrics:

* `store.cassandra.omitted_old_reads`
* `store.cassandra.read_queue_full` 

### remove cassandra index pruning
until we figure out a better mechanism. **disk usage may grow more if you have heavy churn** #765 #800 #816 

## swim (memberlist) settings
we now allow tweaking many swim settings via a new swim config section, and also the **bind-addr property has moved from cluster section to swim section**
see #760 in particular ae3c5daafb39b23b7879d3d6876e6df1e309f05e and 81eea5f5174efe53c6fdb81aa9984ffdae1eed12 
also relevant is the new `gossip-to-the-dead-time` setting which can help with recovering from split brains.

# non-breaking changes

## stats, logging, instrumentation, profiling
* add opentracing instrumentation using jaeger #709 , #713 , #715, #758 , 
 305752585ffa8bb4a9c9d180fbf406da6e9c45fe, 736d2db641b5a8a99fd9c0fc870fcd13206861d2, #732, ea9f56f4ce613acf4c4cd6c5a92ca6ea6198ce84
* fix the cache bug oppressed stats #721 
* consistently report any recoverable runtime faults via metrics and logs. `metrictank.stats.$environment.$instance.recovered_errors.*.*.*` 760bd0621a527c28ba966e267352202edf7a76f9 
* better mutex and block pprof endpoints #737 
* report accurate mean instead of an approximation #744 

## input plugins
* carbon : like graphite, strip leading "." if any from metric key #694 . this fixes an index crash bug #668 

## index
* find improvements #655 
* stop index pruning from locking the index during the entire operation, which was slowing down requests. #787 
* fix CassandraIdx.Init error handling 31990fba2e14f6f18605b6c40f557c8a0a1deaed

## tools
* Whisper importer aggregate conversion and various improvements and fixes #712 , #720 , #743, #752, #793, #814  
* mt-replicator-via-tsdb : use the kafkaMdm input plugin to consume from kafka + various clustering changes related to it #723 
* mt-index-cat tags valid filter #817 
* mt-kafka-mdm-sniff-out-of-order improvements #754 
* mt-index-cat functions for showing age and rounding of durations #763 
* deprecate old mt-index tools and remove mt-replicator because it's not reliable. #783 

## http API
* proper statuscode when render failed #718 
* add `maxSeries` #742
* refactor aggregation function api #771 
* Support for `groupByTags` and `aliasByTags` #780 
* remove from adjustment for clustered requests, for more consistent output #767 and to fix this bug:  "include old metricdefs up to 24h" prevents from new higher res data to become visible #380 
* properly propagate request cancellations through the cluster and cancel work-in progress. #728 
* graphite-compatible msgpack support #789 
* proxy /functions to graphite #815 

## dashboard
* make the dashboard multi-instance capable + separate plots for partition lag vs persist partition lag #722 
* update dashboard 8f356d74

## storage & chunk cache
* make cassandra schema optional via `cassandra-create-keyspace` flag, useful when provisioning clusters. e9ad4d87164b2adb53dc9cd3bca4ef508396827f , e9ad4d87164b2adb53dc9cd3bca4ef508396827f, ac401e7ef7a2d1f3368e7425e72ccb4f516ebfeb, fa885021807df256d7220b7276e385f8a5c74b4f, 831127742d2650e787faf2670c0c77f681f69f8c, d16ca0f66e675348410d2aeb49caf1de064ec626 
* out of order chunks in chunkCache leading to all kinds of trouble #733 
* fix a config bug that was causing reorderbuffer not to activate #756 
* apply cassandra-timeout setting to insert queries #778 
* Clear cache api #555 
* make reorderbuffer and garbage collection work better together. #781 , fixes crash bug #776 
* update gocql a04083fdd997d90c9d0009b0a01ffcc8f7c0f943 #778 

## clustering
* various cluster readyness / priority / initialisation fixes + new cluster dashboard #717
* update memberlist to post v0.1.0 7f40597570638873ad2cdb830da965b595e6571f 
* chaos testing #760 

## meta 
* rebrand: raintank -> GrafanaLabs and repository move #738
* contribution guidelines #740
* circleci build optimizations and tweaks. f2df73a8b30abe62eef0c0ecc17002014b4c9d6b, e994819b2359e4e96686612080b4333c458e2d56, #757, e83343094ef1939ae1798beadbe5c6492c4fd188
* use dep instead of govendor 28c02a7164e5baeb65f69233ea307cf06a70e01b (via #760) 
* builds: go1.8 -> 1.9 #712 
* update dependencies leveldb #785, globalconf #786 
* developer documentation #805
* reorganize the scripts, add various code quality and linting steps to CI, upgrade to circleci 2.0  #803 

# 0.7.4 : point reordering and various improvements and tools. Nov 1, 2017

## data server
* support more from/to specifications, honor timezone controls #682 
* expr panic fix #676
* allow `target[]=foo` specification #688 
* measure duration of plan.Run() #689 
* queries should fail when shards are missing. fixes #670. new config option for min-available-shard
* fix clustered consolidateBy() requests  #707 
* support from/to patterns for find query #708 

## cassandra
* update TWCS settings #690 
* fix clustered scenario schema in cassandra doc  (#679)
* set gc_grace_seconds to the compaction_window_size #700 
* also track writeQueue len on puts. fix #701 

## index
* Avoid unnecessary string allocations during memory-idx.Add #687 , #692 

## reorder buffer (new!)
* reorder buffer to allow for some data to arrive out of order. #675 

## misc
* remove usage reporting from MT #666 
* this might fix an OOM problem in cache.searchForward #696 

## clustering
* Adds an endpoint to post cluster peers to #680 

## tools
* new mt-replicator tool. consumes from local kafka cluster and publishes data to a remote tsdb-gw server. #645, #691
* Improvements on whisper importer #704 

# 0.7.3: bugfix release. Jun 29, 2017

# clustering 

* fix partial data in clustered setups #652

# built in graphite processing library 

* fix for hitcount #644
* support the full char set that graphite supports for metric names #651 
* fix up some target naming #667
* implement nudging similar to graphite #647 

# other

* various small fixes to log messages

# 0.7.2. May 24, 2017

## data server / api
* support a minimal built-in graphite function processing api, proxying to graphite what we cannot do ourselves.
currently supports alias, aliasByNode, aliasSub, avg, divideSeries, perSecond, scale, sum, transformNull and consolidateBy (which now intelligently controls runtime consolidation and archive selection for consolidated archives).  #575, #623 #637 #640 #641 #643 
* allow metrictank to run as a graphite-web cluster node. by using graphite-web we get more stability and performance compared to graphite-api. #611 #616 #633 
* misc fixes for gzip middleware #619 #621 

## clustering
* fix aggregation handling in clustered setups #602 
* fix cluster api errors not logging properly #603
* configurable "drop first chunk" behavior for write nodes #632 

## index
* make index deletes much faster #606, #609 

## tools etc
* fix mt-kafka-mdm-sniff crash bug #612
* add tool to see which metrics are out of order #626 
* default to cassandra tokenaware-hostpool-epsilon-greedy in all tools and when no config file used. #638 

## build and packaging
* use go1.8.1 instead of go1.8rc2 #600 
* mark /etc/metrictank as config dir, fixes a bug where package upgrades would overwrite config files #627

also some docs updates

# 0.7.1. Apr 7, 2017

# data server, index and instrumentation
* be more graphitey in terms of consolidation, and configuration of schemas and retentions. #534  , #570 
* Improve http stats + implement gzip responses #548 
* drastic improves to how we maintain the index in cassandra (later replaced: #558, #560 , #566) . disable cass updates (for secondary nodes) #565.  fix lastSave vs lastUpdate discrepancy #574 
* support msgp output for render responses #562 
* instrument how many series are being filtered from responses #551 
* fix deleting from mixed leaf/branch nodes from index #554 
* replace request limit options with new hard/soft point limits #577
* support multiple raw intervals per storage schema #588 
* remove seenAfter offset (24h buffer) which was sometimes annoying with series having effects too long (e.g. old resolution showing up after having enabled different resolution) #595 

# clustering
* remove state transfer between nodes #535 
* support configuring http client timeout for inter-cluster requests #542 
* introduce priority system for controlling which instance satisfies requests #541, #546 
* ensure nodes can only connect to clusters with the same clusterName #597 

# tools 
* make mt-store-cat more flexible and powerful. #590
* fix mt-kafka-mdm-sniff garbled output. #524 
* add tool for migrating index from one cass cluster to another #527 
* Add tools for cross-cluster split-metric migrations #529
* add filter options to mt-kafka-mdm-sniff #531 
* mt-replicator improvements #523
* add mt-update-ttl tool #515 
* whisper import tools #533 
* add tools to explain schemas and aggregations. #591, 1f7160bfe720c35abbdad5264b03d3c8389f2b9e

# docs
* add an FAQ to docs. #517 
* various doc updates

# builds 
* Include tools in all packages / docker image #585 
* set proper exit code when tool building fails #593 

# 0.7.0. Feb 7, 2017

This overview does not cover _all_ changes, just the major ones.
Similarly, there may be more PR's involved to a given feature beside the mentioned ones.

## clustering
- implement sharded cluster using partitioning #400 #472
- gossip based peer discovery #459 
- process old metricPersist messages before consuming metrics. this makes it easier to safely run statically configured clusters. #452 #485 
- fix metricpersist handling for aggregated metrics #507 
- make secondary nodes also GC #269

## storage
- new chunk format that contains chunkspan. integrates seamlessly with older chunks. #418 
- chunk cache as a more effective way for metrictank to cache hot in-memory data, complements the ringbuffers which can now retain less data #417 #455 #461 
- use different tables based on TTL. #382 #484 
- set default cassandra keyspace to "metrictank" #460 
- update gocql

## stats
- new internal metrics system, replacing statsd[aemon], more performant, and re-organized metrics tree + new dashboard #384
- instrument kafka-mdm metrics #448

## build & deploy
- golang version 1.6 -> 1.8rc2
- CI: be more strict (go vet, gofmt -s, vendor health) #405 
- multiple docker stacks for some different scenarios (standard, dev, clustered, etc) with script to important appropriate dashboards etc. #413  

## index
- remove branch vs leaf restriction. #490 
- various bugfixes. remove ES index as it was discouraged and not well maintained. better metrics (measure number of entries, split up updates vs adds, sync index ops vs background ops) #504 

## api
- escape series names #408 #410 

## tools

metrictank now comes with helper tools:
- mt-index-migrate-050-to-054 #451 
- mt-index-migrate-06-to-07 #425 #451 
- mt-index-cat #437 
- mt-store-cat #458 
- mt-replicator #435 
- mt-view-boundaries #495 
- mt-kafka-mdm-sniff #393, #496 

## inputs
- carbon-in: better metrics2.0 support #491
- update sarama kafka library, which fixes compression support #428

## config & docs
- various updates to configs docs
- default to cassandra index. #411, #449
- standardize on default raw chunkspan 10min and numchunks 7

# 0.6.0

- adopt semver
- removal of kafkamdam and nsq input plugin (though nsq as clustering bus is still supported)
- move http api/listener configuration to new section
- removal of `/get` endpoint which is no longer used
- use `/node` instead of `/api`, contains more info
- huge refactor of api code
- big refactor of input plugin code (#376)
- include notifier type in notifier metrics
- switch default policy to `tokenaware,hostpool-epsilon-greedy` (#374)
- optimize CI runs, add benchmarks to CI, optimize chunk totalPoints tracking (#385, #386)
- support listening on https
- idx fixes wrt persistence and memory structures getting out of sync when trying to add bad metrics (#398)

# 0.5.8

- support environment variables for configuration, using MT_ prefix
- fix data sometimes not being properly returned (#333)
- support for cassandra SSL and auth (#367)
- use instance-id for statsd metrics, not hostname (#368)
- script for maintaining ES index (#369)
- report "Cannot achieve consistency level" errors
- track GC heap objects. (#340)

# 0.5.7

- support for cassandra retries & configurable host selection policy
- better cassandra metrics
- end to end testing in CircleCI.
- docker entrypoint script now actually verifies it can hold a connection open for a few seconds.

# 0.5.6

- instrument cassandra errors & timeouts. update dashboard
- update gocql cassandra libray. no impact expected.
- better docs, especially around operations. include overview diagram
- make heap profile trigger more simple to use
- simplify docker stack: reuse existing images, remove elasticsearch
- dashboard: some better charts, fix some units

# 0.5.5

test. same as 0.5.4

# 0.5.4

index:
- various improvements, doc updates
- support pruning & deleting from index (causes a slight increased load on the cassandra backend)
- bugfix: abort if metricIndex failed to initialize #325 

other:
- improvements to clustering, operations, cassandra docs and install guides. in particular deb install guide fixes
- filter metric search results by comparing from TS a07007ab8d4a22b122bbc5f9fadb51480e1c5b0c to lastUpdate field from metricdefinition

# 0.5.3

index:
- add cassandra backed metric index
- better metric names (+ update dashboard)
- add metric for items in ES index retry buffer (+ update dashboard)
- dashboard: show index&cassandra durations better
- fix memoryIdx bug (39bfcc892e2cd3a5f670f5e989638430838ce1f8)
- cleanups (no impact)

doc:
- split up installation guide into 3 different guides, and improve them

config:
- fix index config validation (ca0ff636f2dc39c005373c21be78aa64def4299d)
- configurable cassandra keyspace
- better configs for docker stack vs in package
- default consumer-fetch-min = 1
- make statsd instrumentation optional, e.g. remove dependency on statsd.
- set consumer-fetch-default to sarama default of 32768
- fix bug that was resetting consumer-fetch-default to 100

core:
- fix null checking in series merging (e22236e2758e120dc4ffe5fbd8cf48fce67773ce)
- move kafkaMdm input and kafka cluster plugin to manage their own offsets, instead of using sarama-cluster for consumergroups which was pointless.
- display request information when it causes a panic.

# 0.5.2

new metadata index
- refactored metadata& caching into new index package
- provide memory and elasticsearch backend

core:
- allow adding points to the latest, but already saved chunk
- various persister changes. #263 
- fix carbon input which was broken since new metrics2.0 version.
- configurable http limits. fix #268
- add ES index fail metrics
- truly validate metrics in ingest

builds & packaging:
- refactor circleCI building
- (graphite-raintank is now graphite-metrictank)
- fix packages for centos 6 & 7, debian/jessie
- add a docker stack
- auto push docker image to dockerhub when circleci builds
- switch to AGPL
- include include storage-schemas.conf in packages

docs:
- various docs update, put docs table in readme
- simplify readme
- add metrictank dashboard, and add [on grafana.net](https://grafana.net/dashboards/279)

# 0.5.1

- rename metric_tank/metrics_tank to metrictank. this changes all the statsd metrics(!)
- add kafka clustering transport next to nsq
- upgrade to new, proper metrics2.0 schema, which changes how metricId's are generated, but ...
- ....support series merging to seamlessly combine data for the same metric living under different metricdefs ff7a9bc14caae4d4128aaf9a7de635106c70e4a0
- instrument input decoding errors
- adopt govendor for go vendoring #197

config:
- proper sample config that actually makes sense
- make configuration of input plugins more elegant
- kafka offset: default to newest (#236)
- switch fallback schema from DateTieredCompactionStrategy to TimeWindowCompactionStrategy #249 
- configurable CQL proto version
- kafka-mdm: expose performance tuneables

doc & packaging:
- various bugfixes in packaging scripts and packages.
- build both ubuntu14.04 and 16.04 packages, and centos 6 & 7
- doc updates, especially around "data knobs", usage reporting, consolidation, http api, cassandra, index plugin writing, clustering and readme.

# v0.5.0

This is the first "release", and is the culmination of a lot of work.
Some highlights of recent developments leading up to this version:

rename metric-tank -> metrictank
promote metrictank to top level of repo and move other stuff out, start versioning metric schema's in other repo
add kafka-mdm, kafka-mdam and carbon input next to nsq.
accept empty inputs instead of calling log.Fatal #226
don't busy loop and max out cpu if metricpersist production has issues
make sure metricpersist msgs can't go over nsq's max size. #224
