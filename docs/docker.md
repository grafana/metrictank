# docker stacks

You may have already followed [our docker quickstart guide](https://github.com/grafana/metrictank/blob/master/docs/quick-start-docker.md) but in fact,
the metrictank repository contains a variety of docker stacks for different purposes.

They can be started by cd'ing into their directory and running `docker-compose up --force-recreate -V`

Here's an overview.
Note:
* not everything is detailed here. Plus this needs some more work.  For specifics on any setup, please refer to the sources.
* every environment comes with grafana and cassandra unless otherwise noted
* every environment automatically provisions useful datasources and dashboards unless otherwise noted

### docker-standard

plain, basic environment.
uses metrictank docker image (with baked in binaries and configs), statsdaemon.
Comes with 1 dashboard: the metrictank dashboard

[see our quickstart guide](https://github.com/grafana/metrictank/blob/master/docs/quick-start-docker.md)

### docker-dev

Similar to docker-standard, but custom metrictank build is loaded, custom configs from scripts/config
also: tag support, main+extra dashboards

### docker-dev-bigtable

Same as docker-dev but instead of using cassandra as the backend store and index store it uses Bigtable via a bigtable emulator.

### docker-dev-scylla

Same as docker-dev but using scylladb instead of cassandra

### docker-dev-custom-cfg-kafka

Similar to docker-dev in terms of build and configuration.
Also:
* kafka input and clustering backend
* short chunkspan & numchunks 
  (benefits: easy to trigger mem and mem_and_cass requests, frequent cass saves, notifier messages etc)
* prometheus monitoring and jaeger tracing
* caddy-proxied endpooints/datasources to check with different org-id auth headers
* tsdb-gw

### docker-cluster

Similar to docker-dev-custom-cfg-kafka

* uses a cluster of 4 MT's (with 2x replication)
* separate graphite monitoring server

[see our docker-cluster quickstart guide](https://github.com/grafana/metrictank/blob/master/docs/quick-start-docker-cluster.md)

### docker-chaos

Similar to docker-cluster but used for chaos testing


### docker-cosmosdb

Used for testing against cosmosDB. Note: expects you to have a reachable cosmosdb and adjust settings accordingly.
Does not auto-load datasources or dashboards.
