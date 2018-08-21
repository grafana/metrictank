# Cassandra and scyllaDB

We run what is currently the latest version of Cassandra, 3.11
Other well tested versions are 3.0.8
It should still work with 2.2 (we used to run on 2.2.3 for a while), with some schema and compaction tweaks, see below.
People have reported success running [scylladb](https://www.scylladb.com/) as Cassandra alternative and we provide schemas for that as well.

Note that we have a Cassandra data storage plugin, and a Cassandra index (metadata) plugin. Each has their own config section, metrics, etc and can be used independently.

## Configuration

[Cassandra storage configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md#metric-data-storage-in-cassandra)
[Cassandra index configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md#in-memory-cassandra-backed)

The defaults are fine, especially for test/development setups.

Here are some settings that are worth elaborating on:

* `retries`: turning up this value will allow the data persistence layer to transparantly retry queries, should they error or time out.  The consequence of this is that
  cassandra gets may take longer then what the timeout value is set to.  Note that queries may still be aborted due to an error or timeout without retrying as many times as the
  configuration allows.  This is because based on your host-selection-policy, hosts may be marked offline if they timeout.  See [gocql/812](https://github.com/gocql/gocql/issues/812).
  So just be aware of this as you configure your host selection policy.

## Schema

Metrictank comes with these schema files out of the box:

* [/etc/metrictank/schema-store-cassandra.toml](https://github.com/grafana/metrictank/blob/master/scripts/config/schema-store-cassandra.toml)
* [/etc/metrictank/schema-idx-cassandra.toml](https://github.com/grafana/metrictank/blob/master/scripts/config/schema-idx-cassandra.toml)
* [/usr/share/metrictank/examples/schema-store-scylladb.toml](https://github.com/grafana/metrictank/blob/master/scripts/config/schema-store-scylladb.toml)
* [/usr/share/metrictank/examples/schema-idx-scylladb.toml](https://github.com/grafana/metrictank/blob/master/scripts/config/schema-idx-scylladb.toml)

These files initialize the database with the keyspace and table schema. For the store and index plugins respectively.
By default it will use the the files in /etc/metrictank.
The keyspace to use can be set in the configuration using the "cassandra-keyspace" option, the default is "metrictank":

For clustered scenarios, you may want to tweak the schema:

```
CREATE KEYSPACE IF NOT EXISTS metrictank WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3'}  AND durable_writes = true;
```

If you need to run Cassandra 2.2, the backported [TimeWindowCompactionStrategy](https://github.com/jeffjirsa/twcs) is probably your best bet.
See [issue cassandra-9666](https://issues.apache.org/jira/browse/CASSANDRA-9666) for more information.
You may also need to lower the cql-protocol-version value in the config to 3 or 2.


## Data persistence

saving of chunks is initiated whenever the current time reaches a timestamp that divides without remainder by a chunkspan.
Raw data has a certain chunkspan, and aggregated (rollup data) has chunkspans too (see [config](https://github.com/grafana/metrictank/blob/master/docs/config.md#data)) which is
why periodically e.g. on the hour and on every 6th our you'll see a burst in chunks being added to the write queue.
The write queue is then gradually drained by the persistence workers.


## Write queues

Tuning the write queue is a bit tricky for now.
Basically you have to make sure that `number of concurrent writers` times `write queue size` is enough to queue up all chunk writes that may occur at any given time.
Chunk writes that may occur at any given time is usually `number of unique series you have` times (`number of rollups` * 4 + 1)
There's also an upper bound for how large these queues can get.
See [this ticket](https://github.com/grafana/metrictank/issues/125) for more information and discussion.
The advent of the new kafka input will probably resolve a lot of the constraints. Both for the lower and upper bound.


Just make sure that the queues are able to drain when they fill up. You can monitor this with the Grafana dashboard.

