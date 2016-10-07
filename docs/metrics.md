# Overview of metrics
(only shows metrics that are documented. generated with [metrics2docs](github.com/Dieterbe/metrics2docs))

* `add_to_saved_chunk`:  
points received - by a secondary node - for the most recent chunk when that chunk
has already been saved by a primary.  A secondary can add this data to its chunks.
* `add_to_saving_chunk`:  
points received - by the primary node - for the most recent chunk
when that chunk is already being saved (or has been saved).
this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
your (infrequent) updates.  The primary won't add them to its in-memory chunks, but secondaries will
(because they are never in "saving" state for them), see below.
* `bytes_alloc.incl_freed`:  
a counter of total amount of bytes allocated during process lifetime. (incl freed data)
* `bytes_alloc.not_freed`:  
a gauge of currently allocated (within the runtime) memory.
it does not include freed data so it drops at every GC run.
* `bytes_sys`:  
the amount of bytes currently obtained from the system by the process.  This is what the profiletrigger looks at.
* `cluster.promotion_wait`:  
how long a candidate (secondary node) has to wait until it can become a primary
When the timer becomes 0 it means the in-memory buffer has been able to fully populate so that if you stop a primary
and it was able to save its complete chunks, this node will be able to take over without dataloss.
You can upgrade a candidate to primary while the timer is not 0 yet, it just means it may have missing data in the chunks that it will save.
* `gc_metric`:  
the amount of times the metrics GC is about to inspect a metric (series)
* `metrics_active`:  
the amount of currently known metrics (excl rollup series), measured every second
* `metrics_too_old`:  
points that go back in time.
E.g. for any given series, when a point has a timestamp
that is not higher than the timestamp of the last written timestamp for that series.
