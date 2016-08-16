# Clustering

## Underlying storage
for clustering and HA of the underlying storage, we of course can simply rely on Cassandra.

## Metrictank redundancy and fault tolerance

for metrictank itself you can achieve redundancy and fault tolerance by running multiple instances.
One of them needs to have the primary role, and the role can dynamically be reassigned (see http api docs)
There's 2 transports for clustering (kafka and NSQ), and it's just used to send messages by the primary to the others,
about which chunks have been saved to cassandra.
Instances should not become primary when they have incomplete chunks (though in worst case scenario, you might
have to do just that).  So they expose metrics that describe when they are ready to be upgraded.
Notes:
* all instances receive the same data and have a full copy in this case.
* primary role controls who writes chunks to cassandra. metadata is updated by all instances independently.

when one primary is down you need to be careful about when to promote a secondary to primary:

* after you see the "starting data consumption" log message for a primary, data consomuption starts. this timestamp is important.
* look at your largest chunkSpan. secondary can only be promoted when a new interval starts for the largest chunkSpan. intervals start when clock unix timestamp divides without remainder by chunkSpan. How long you should wait is also shown (in seconds) via the `cluster.promotion_wait` metric.
* of course there are other factors: any running primary should be depromoted and have saved its data to cassandra, all metricPersist message should have made it through NSQ into the about-to-be-promoted instance.


## Horizontal scaling

As for load balancing / partitioning / scaling horizontally, metrictank has no mechanism built-in to make this easier.
You should be able to run multiple instances and route a subset of the traffic to each, by using proper
partitioning in kafka. Instances could either use the same metadata index and cassandra cluster, or different ones, should all work.
However we have not tried this yet, simply because we haven't needed this yet: single instances can grow quite large.
This will be an area of future work, and input is welcomed.

