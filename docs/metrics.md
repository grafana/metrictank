# Overview of metrics
(only shows metrics that are documented. generated with [metrics2docs](github.com/Dieterbe/metrics2docs))

* `metrics_too_old`:  
points that go back in time.
E.g. for any given series, when a point has a timestamp
that is not higher than the timestamp of the last written timestamp for that series.
* `add_to_saving_chunk`:  
points received - by the primary node - for the most recent chunk
when that chunk is already being saved (or has been saved).
this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
your (infrequent) updates.  The primary won't add them to its in-memory chunks, but secondaries will
(because they are never in "saving" state for them), see below.
* `add_to_saved_chunk`:  
points received - by a secondary node - for the most recent chunk when that chunk
has already been saved by a primary.  A secondary can add this data to its chunks.
* `metrics_active`:  
the amount of currently known metrics (excl rollup series), measured every second
* `gc_metric`:  
the amount of times the metrics GC is about to inspect a metric (series)
