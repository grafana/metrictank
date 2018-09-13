# Quick start for docker-cluster

The docker-cluster image is similar to docker-standard, but with the following changes and features:

* Cluster of 4 metrictanks (with 2x replication)
* Separate Graphite monitoring server
* The metrictank binary is mounted from `metrictank/build`, so `make bin` must be run before spinning up the stack (benefits: speeds up development and testing)
* Loads custom configurations and scripts
* Supports tags
* More dashboards are available
* Kafka input and clustering backend
* Short chunkspan & numchunks (benefits: easy to trigger mem and mem_and_cass requests, frequent cass saves, notifier messages etc)
* Prometheus monitoring
* Jaeger tracing
* caddy-proxied endpooints/datasources to check with different org-id auth headers
* tsdb-gw


The following programs are required to build and run the docker-cluster image:

* [Docker](https://docs.docker.com/install/)
* [docker-compose](https://docs.docker.com/compose) >= version 1.6
* [Go](https://golang.org/doc/install) (needed to build metrictank)
   * Ensure that `$GOPATH/bin` is added to your `$PATH`

## Getting the repository

Pull down the repository and its dependencies: 

```
go get github.com/grafana/metrictank/...
cd $GOPATH/src/github.com/grafana/metrictank
```

## Build metrictank

Build metrictank and the docker images:
```
make
```

`make` uses/creates the directory `$GOPATH/src/github.com/grafana/metrictank/build`. If you tried to spin up the stack prior to building metrictank then docker already created this directory, as root, and it will need to be deleted (then run `make` again).

## Bring up the stack

The stack will listen on the following ports:

* 80    tcp (carbon)
* 2003  tcp (metrictank's carbon input)
* 2181  tcp (kafka)
* 3000  tcp (grafana's http port)
* 6060  tcp (metrictank's internal endpoint)
* 6061  tcp (caddy)
* 6062  tcp (caddy)
* 6063  tcp (caddy)
* 6831  udp (jaeger)
* 8080  tcp (the graphite query endpoint)
* 8081  tcp (caddy)
* 8082  tcp (caddy)
* 8125  udp (statsd endpoint)
* 9042  tcp (cassandra)
* 9090  tcp (prometheus)
* 9092  tcp (kafka)
* 9100  tcp (prometheus node exporter)
* 9999  tcp (kafka)
* 16686 tcp (jaeger)

If you already have something else listening on those ports (such as a carbon or grafana server), shut it down, as it will conflict.


Inside your copy of the repository, you can bring up the stack like so:

```
cd docker/docker-cluster
docker-compose up
```

If this gives you the error `service 'version' doesn't have any configuration options`,
your version of `docker-compose` is too old and you need to update to >=1.6.

Wait until the stack is up.

## Working with Grafana and metrictank

In your browser, open Grafana at http://localhost:3000 (or your docker-machine address) and log in as `admin:admin`.  
If Grafana prompts you to change the password, you can skip it, since it doesn't matter for a local test setup.

### Working with the cluster

This stack has some unique properties not found in the standard stack, such as multiple data sources to choose from including, but not limited to:
* monitoring and prometheus for data about the stack
* metrictank to see data in metrictank
* graphite to see data in metrictank via graphite
  
When using [fakemetrics](https://github.com/raintank/fakemetrics) to generate data you should now use kafka
* `fakemetrics feed --kafka-mdm-addr localhost:9092`
* stats will still show up under `some.id.of.a.metric.*`

If anything doesn't work, please let us know via a ticket on github or reach out on slack. See
[Community](https://github.com/grafana/metrictank/blob/master/docs/community.md)


## Shut down the stack

You can shut down the stack by just hitting `Ctrl-C` on your running `docker-compose up` command.


You can remove the containers like so:
```
docker-compose stop
```

To clean up all data so you can start fresh, run this after you stopped the stack:
```
docker rm -v $(docker ps -a -q -f status=exited)
```
This will remove the stopped containers and their data volumes.