CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS raintank.metruc (
    key string,
    ts timestamp,
    data blob,
    PRIMARY KEY (key, ts)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (ts DESC)
