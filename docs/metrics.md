# Overview of metrics
(only shows metrics that are documented. generated with [metrics2docs](github.com/Dieterbe/metrics2docs))

* `api.get_target`:  
how long it takes to get a target
* `api.iters_to_points`:  
how long it takes to decode points from a chunk iterator
* `api.request_handle`:  
how long it takes to handle a render request
* `api.requests_span.mem`:  
the timerange of requests hitting only the ringbuffer
* `api.requests_span.mem_and_cassandra`:  
the timerange of requests hitting both in-memory and cassandra
* `cache.ops.chunk.add`:  
how many chunks were added to the cache
* `cache.ops.chunk.evict`:  
how many chunks were evicted from the cache
* `cache.ops.chunk.hit`:  
how many chunks were hit
* `cache.ops.metric.add`:  
how many metrics were added to the cache
* `cache.ops.metric.evict`:  
how many metrics were evicted from the cache
* `cache.ops.metric.hit-full`:  
how many metrics were hit fully (all needed chunks in cache)
* `cache.ops.metric.hit-partial`:  
how many metrics were hit partially (some of the needed chunks in cache, but not all)
* `cache.ops.metric.miss`:  
how many metrics were missed completely (none of the needed chunks in cache)
* `cluster.notifier.kafka.message_size`:  
the sizes seen of messages through the kafka cluster notifier
* `cluster.notifier.kafka.messages-published`:  
a counter of messages published to the kafka cluster notifier
* `cluster.notifier.kafka.partition.%d.offset`:   
The current offset for the partition (%d) that we have consumed.
* `cluster.notifier.kafka.partition.%d.log_size`:   
The size of the kafka partition, aka the newest available offset.
* `cluster.notifier.kafka.partition.%d.lag`:   
How many messages (chunkWriteRequests) Kafaka has that we have not yet consumed.
* `cluster.notifier.nsq.message_size`:  
the sizes seen of messages through the nsq cluster notifier
* `cluster.notifier.nsq.messages-published`:  
a counter of messages published to the nsq cluster notifier
* `cluster.notifier.all.messages-received`:  
a counter of messages received from cluster notifiers
* `cluster.decode_err.join`:  
a counter of json unmarshal errors
* `cluster.decode_err.merge_remote_state`:  
a counter of json unmarshal errors
* `cluster.decode_err.update`:  
a counter of json unmarshal errors
* `cluster.events.join`:  
how many node join events were received
* `cluster.events.leave`:  
how many node leave events were received
* `cluster.events.update`:  
how many node update events were received
* `cluster.self.partitions`:  
the number of partitions this instance consumes
* `cluster.self.promotion_wait`:  
how long a candidate (secondary node) has to wait until it can become a primary
When the timer becomes 0 it means the in-memory buffer has been able to fully populate so that if you stop a primary
and it was able to save its complete chunks, this node will be able to take over without dataloss.
You can upgrade a candidate to primary while the timer is not 0 yet, it just means it may have missing data in the chunks that it will save.
* `cluster.self.state.primary`:  
whether this instance is a primary
* `cluster.self.state.ready`:  
whether this instance is ready
* `cluster.total.partitions`:  
the number of partitions in the cluster that we know of
* `cluster.total.state.primary-not-ready`:  
the number of nodes we know to be primary but not ready (total should only be in this state very temporarily)
* `cluster.total.state.primary-ready`:  
the number of nodes we know to be primary and ready
* `cluster.total.state.secondary-not-ready`:  
the number of nodes we know to be secondary and not ready
* `cluster.total.state.secondary-ready`:  
the number of nodes we know to be secondary and ready
* `idx.cassadra.query-delete.ok`:  
how many delete queries for a metric completed successfully (triggered by an update or a delete)
* `idx.cassadra.query-insert.ok`:  
how many insert queries for a metric completed successfully (triggered by an add or an update)
* `idx.cassandra.add`:  
the duration of an add of one metric to the cassandra idx, including the add to the in-memory index, excluding the insert query
* `idx.cassandra.delete`:  
the duration of a delete of one or more metrics from the cassandra idx, including the delete from the in-memory index and the delete query
* `idx.cassandra.error.cannot-achieve-consistency`:  
a counter of the cassandra idx not being able to achieve consistency for a given query
* `idx.cassandra.error.conn-closed`:  
a counter of how many times we saw a connection closed to the cassandra idx
* `idx.cassandra.error.no-connections`:  
a counter of how many times we had no connections remaining to the cassandra idx
* `idx.cassandra.error.other`:  
a counter of other errors talking to the cassandra idx
* `idx.cassandra.error.timeout`:  
a counter of timeouts seen to the cassandra idx
* `idx.cassandra.error.too-many-timeouts`:  
a counter of how many times we saw to many timeouts and closed the connection to the cassandra idx
* `idx.cassandra.error.unavailable`:  
a counter of how many times the cassandra idx was unavailable
* `idx.cassandra.prune`:  
the duration of a prune of the cassandra idx, including the prune of the in-memory index and all needed delete queries
* `idx.cassandra.query-delete.exec`:  
time spent executing deletes (possibly repeatedly until success)
* `idx.cassandra.query-delete.fail`:  
how many delete queries for a metric failed (triggered by an update or a delete)
* `idx.cassandra.query-insert.exec`:  
time spent executing inserts (possibly repeatedly until success)
* `idx.cassandra.query-insert.fail`:  
how many insert queries for a metric failed (triggered by an add or an update)
* `idx.cassandra.query-insert.wait`:  
time inserts spent in queue before being executed
* `idx.cassandra.update`:  
the duration of an update of one metric to the cassandra idx, including the update to the in-memory index, excluding any insert/delete queries
* `idx.memory.add`:  
the duration of a (successful) add of a metric to the memory idx
* `idx.memory.add.fail`:  
the number of failed additions to the memory idx
* `idx.memory.add.ok`:  
the number of successful additions to the memory idx
* `idx.memory.delete`:  
the duration of a delete of one or more metrics from the memory idx
* `idx.memory.find`:  
the duration of memory idx find
* `idx.memory.get`:  
the duration of a get of one metric in the memory idx
* `idx.memory.list`:  
the duration of memory idx listings
* `idx.memory.prune`:  
the duration of successful memory idx prunes
* `idx.memory.update`:  
the duration of (successful) update of a metric to the memory idx
* `idx.memory.update.ok`:  
the number of successful updates to the memory idx
* `idx.metrics_active`:  
the number of currently known metrics in the index
* `mem.to_iter`:  
how long it takes to transform in-memory chunks to iterators
* `memory.bytes.obtained_from_sys`:  
the number of bytes currently obtained from the system by the process.  This is what the profiletrigger looks at.
* `memory.bytes_allocated_on_heap`:  
a gauge of currently allocated (within the runtime) memory.
* `memory.gc.cpu_fraction`:  
how much cpu is consumed by the GC across process lifetime, in pro-mille
* `memory.gc.heap_objects`:  
how many objects are allocated on the heap, it's a key indicator for GC workload
* `memory.gc.last_duration`:  
the duration of the last GC STW pause in nanoseconds
* `memory.total_bytes_allocated`:  
a counter of total number of bytes allocated during process lifetime
* `memory.total_gc_cycles`:  
a counter of the number of GC cycles since process start
* `metric_invalid`:  
a count of times a metric did not validate
* `metrics_decode_err`:  
a count of times an input message (MetricData, MetricDataArray or carbon line) failed to parse
* `store.cassandra.chunk_operations.save_fail`:  
counter of failed saves
* `store.cassandra.chunk_operations.save_ok`:  
counter of successful saves
* `store.cassandra.chunk_size.at_load`:  
the sizes of chunks seen when loading them
* `store.cassandra.chunk_size.at_save`:  
the sizes of chunks seen when saving them
* `store.cassandra.chunks_per_row`:  
how many chunks are retrieved per row in get queries
* `store.cassandra.error.cannot-achieve-consistency`:  
a counter of the cassandra store not being able to achieve consistency for a given query
* `store.cassandra.error.conn-closed`:  
a counter of how many times we saw a connection closed to the cassandra store
* `store.cassandra.error.no-connections`:  
a counter of how many times we had no connections remaining to the cassandra store
* `store.cassandra.error.other`:  
a counter of other errors talking to the cassandra store
* `store.cassandra.error.timeout`:  
a counter of timeouts seen to the cassandra store
* `store.cassandra.error.too-many-timeouts`:  
a counter of how many times we saw to many timeouts and closed the connection to the cassandra store
* `store.cassandra.error.unavailable`:  
a counter of how many times the cassandra store was unavailable
* `store.cassandra.get.exec`:  
the duration of getting from cassandra store
* `store.cassandra.get.wait`:  
the duration of the get spent in the queue
* `store.cassandra.get_chunks`:  
the duration of how long it takes to get chunks
* `store.cassandra.put.exec`:  
the duration of putting in cassandra store
* `store.cassandra.put.wait`:  
the duration of a put in the wait queue
* `store.cassandra.rows_per_response`:  
how many rows come per get response
* `store.cassandra.to_iter`:  
the duration of converting chunks to iterators
* `tank.add_to_closed_chunk`:    
points received for the most recent chunk when that chunk is already being "closed",
ie the end-of-stream marker has been written to the chunk.
This indicates that your GC is actively sealing chunks and saving them before you have the chance to send
your (infrequent) updates.  Any points revcieved for a chunk that has already been closed are discarded.
* `tank.chunk_operations.clear`:  
a counter of how many chunks are cleared (replaced by new chunks)
* `tank.chunk_operations.create`:  
a counter of how many chunks are created
* `tank.gc_metric`:  
the number of times the metrics GC is about to inspect a metric (series)
* `tank.metrics_active`:  
the number of currently known metrics (excl rollup series), measured every second
* `tank.metrics_too_old`:  
points that go back in time.
E.g. for any given series, when a point has a timestamp
that is not higher than the timestamp of the last written timestamp for that series.
* `tank.persist`:  
how long it takes to persist a chunk (and chunks preceeding it)
this is subject to backpressure from the store when the store's queue runs full
* `tank.total_points`:  
the number of points currently held in the in-memory ringbuffer
* `input.kafka-mdm.partition.%d.offset`:   
The current offset for the partition (%d) that we have consumed.
* `input.kafka-mdm.partition.%d.log_size`:   
The size of the kafka partition, aka the newest available offset.
* `input.kafka-mdm.partition.%d.lag`:   
How many messages (metrics) Kafaka has that we have not yet consumed.
