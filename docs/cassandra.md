# schema initialisation

by default, metrictank will initialize with the following settings:

```
CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true

CREATE TABLE IF NOT EXISTS raintank.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0
```

These settings are good for development and geared towards Cassandra 2.2.3
Note that `COMPACT STORAGE` is [discouraged as of Cassandra 3.0](http://www.datastax.com/2015/12/storage-engine-30)

For clustered scenarios, you may want to initialize Cassandra yourself with a schema like:

```
CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'NetworkTopologyStrategy', 'us-central1': '3'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS raintank.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0;

```

# write queues

Tuning the write queue is a bit tricky for now.
Basically you have to make sure that `number of concurrent writers` times `write queue size` is enough to queue up all chunk writes that may occur at any given time.
Chunk writes that may occur at any given time is usually `number of unique series you have` times (`number of rollups` * 4 + 1)
There's also an upper bound for how large these queues can get.
See [this ticket](https://github.com/raintank/metrictank/issues/125) for more information and discussion.
The advent of the new kafka input will probably resolve a lot of the constraints. Both for the lower and upper bound.


Just make sure that the queues are able to drain when they fill up. You can monitor this with the Grafana dashboard.

