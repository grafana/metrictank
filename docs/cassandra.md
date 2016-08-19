# Cassandra version

We run what is currently the latest version, 3.0.8, and recommend you do the same.
It also works with 2.2 (we ran on 2.2.3 for a while), with some schema and compaction tweaks, see below.

## Configuration

The default Cassandra configuration is fine, especially for test/development setups.

## Schema

By default, metrictank will initialize Cassandra with the following keyspace and table:

```
CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true

CREATE TABLE IF NOT EXISTS raintank.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
```

These settings are good for development and geared towards Cassandra 3.0

For clustered scenarios, you may want to initialize Cassandra yourself with a schema like:

```
CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'NetworkTopologyStrategy', 'us-central1': '3'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS raintank.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH WITH CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
```

If you need to run Cassandra 2.2, the backported [TimeWindowCompactionStrategy](https://github.com/jeffjirsa/twcs) is probably your best bet.
See [issue cassandra-9666](https://issues.apache.org/jira/browse/CASSANDRA-9666) for more information.
You may also need to lower the cql-protocol-version value in the config to 3 or 2.

## Write queues

Tuning the write queue is a bit tricky for now.
Basically you have to make sure that `number of concurrent writers` times `write queue size` is enough to queue up all chunk writes that may occur at any given time.
Chunk writes that may occur at any given time is usually `number of unique series you have` times (`number of rollups` * 4 + 1)
There's also an upper bound for how large these queues can get.
See [this ticket](https://github.com/raintank/metrictank/issues/125) for more information and discussion.
The advent of the new kafka input will probably resolve a lot of the constraints. Both for the lower and upper bound.


Just make sure that the queues are able to drain when they fill up. You can monitor this with the Grafana dashboard.

