
# clustering

run one primary which writes to cassandra
when one primary is down you need to be careful about when to promote a secondary to primary:

* after you see the "starting data consumption" log message for a primary, data consomuption starts. this timestamp is important.
* look at your largest chunkSpan. secondary can only be promoted when a new interval starts for the largest chunkSpan. intervals start when clock unix timestamp divides without remainder by chunkSpan. How long you should wait is also shown (in seconds) via the `cluster.promotion_wait` metric.
* of course there are other factors: any running primary should be depromoted and have saved its data to cassandra, all metricPersist message should have made it through NSQ into the about-to-be-promoted instance.

