# Clustering

## Underlying storage
for clustering and HA of the underlying storage, we of course can simply rely on Cassandra. It has built-in replication and data partitioning to assure both HA and load balancing.

## Metrictank redundancy and fault tolerance

For metrictank itself you can achieve redundancy and fault tolerance by running multiple instances which receive identical inputs.
One of the instances needs to have the primary role, which means it saves data chunks to Cassandra.   The other instances are secondaries.

Configuration of primary vs secondary:

* statically in the [cluster section of the config](https://github.com/raintank/metrictank/blob/master/docs/config.md#clustering) for each instance.
* dynamically (see [http api docs](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)) should your primary crash or you want to shut it down.

### Clustering transport and synchronisation

The primary sends out persistence messages when it saves chunks to Cassandra.  These messages simply detail which chunks have been saved.
If you want to be able to promote secondaries to primaries, it's important they have been ingesting and processing these messages, so that the moment they become primary,
they don't start saving all the chunks it has in memory, which could be a significant sudden load on Cassandra.

Metrictank supports 2 transports for clustering (kafka and NSQ), configured in the [clustering transports section in the config](https://github.com/raintank/metrictank/blob/master/docs/config.md#clustering-transports)

Instances should not become primary when they have incomplete chunks (though in worst case scenario, you might
have to do just that).  So they expose metrics that describe when they are ready to be upgraded.
Notes:
* all instances receive the same data and have a full copy in this case.
* primary role controls who writes *data* chunks to cassandra. *metadata* is updated by all instances independently and redundantly, see [metadata](https://github.com/raintank/metrictank/blob/master/docs/metadata.md).

### Promoting a secondary to primary

If the primary crashed, or you want to take it down for maintenance you will need to upgrade a secondary instance (the "candidate") to primary status.
This procedure needs to be done carefully:

1) assure there is no primary running.  Demote the primary to secondary if it's running. Make sure all the persistence messages made it through the clustering transport into the 
candidate. Synchronisation over the transport is usually near-instant so waiting a few seconds is usually fine.  But if you want to be careful, assure that the primary stopped sending persistence messages (via the dashboard), and verify that the candidate caught up with the transport (by monitoring the consumption delay from the transport).

2) pick a node that has `cluster.promotion_wait` at zero.  This means the instance has been consuming data long enough to have full data for all the recent chunks that will have to be saved - assuming the primary was able to persist its older chunks to Cassandra.  If that's not the case, just pick the instance that has been consuming data the longest or has a full working set in RAM (e.g. has been for longer than [`chunkspan * numchunks`](https://github.com/raintank/metrictank/blob/master/docs/config.md#data).  (note: when a metrictank process starts, it first does a few maintenance tasks before data consumption starts, such as filling its index when needed)
The `cluster.promotion_wait` is automatically determined based on the largest chunkSpan (typically your coarsest aggregation) and the current time.  From which an instance can derive when it's ready.

3) open the Grafana dashboard and verify that the secondary is able to save chunks 

## Metrictank: Horizontal scaling

As for load balancing / partitioning / scaling horizontally, metrictank has no mechanism built-in to make this easier.
You should be able to run multiple instances and route a subset of the traffic to each, by using proper
partitioning in kafka. Instances could either use the same metadata index and cassandra cluster, or different ones, should all work.
The main problem is combining data together to serve read requests fully.  This is a [work in progress](https://github.com/raintank/metrictank/issues/315)
Any input is welcomed.

