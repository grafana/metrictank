# Quick start for docker-cluster

The docker-cluster image is similar to docker-standard, but with the following changes and features:

* Cluster of 4 metrictanks (with 2x replication)
* Separate Graphite monitoring server
* The metrictank binary is **not** baked in
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

`make` creates the folder `$GOPATH/src/github.com/grafana/metrictank/build`. If you experience a permissions error during `make` it is probably related to this. Try either changing ownership (`chown`) or permissions (`chmod`) on the folder to something your current user is able to read and write, then run `make` again.

You may receive a few build errors at the end relating to QA, ignore them. Ensure the binary `$GOPATH/src/github.com/grafana/metrictank/build/metrictank` was built.

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

In your browser, open Grafana at `http://localhost:3000` (or your docker-machine address) and log in as `admin:admin`.  
If Grafana prompts you to change the password, you can skip it, since it doesn't matter for a local test setup.  

### Sending and visualizing data

In the "+" (Create) menu, hit `Dashboard`.  
This opens the dashboard editor and has a selector open to add a new panel.  Hit "Graph" for graph panel.  
The panel will appear but not contain data yet.  ([Grafana documentation for graph panel](http://docs.grafana.org/features/panels/graph/)).
Click on the title of the panel and hit 'edit'.
In the metrics tab you should see a bunch of metrics already in the root hierarchy:

* `service_is_statsdaemon`: statsdaemon's own internal metrics which it sends to metrictank's carbon port.
* `metrictank`: internal stats reported by metrictank
* `stats`: metrics aggregated by statsdaemon and sent into metrictank every second. Will only show up if something actually sends
  metrics into statsdaemon (e.g. if graphite receives requests directly, you send stats to statsdaemon, etc)


Note that metrictank is setup to track every metric on a 1-second granularity.  If you wish to use it for less frequent metrics,
you have to modify the storage-schemas.conf, just like with graphite.

You can also send your own data into metrictank using the carbon input, like so:

```
echo "example.metric 123 $(date +%s)" | nc localhost 2003
```

Or into statsdaemon - which will compute statistical summaries - like so:

```
echo "hits:1|c" | nc -w 1 -u localhost 8125
```

You can then visualize these metrics in the panel by selecting them.  Note: if you only send single points, you should change the draw mode to point (Display tab)

### Using pre-made dashboards

There is an extensive [dashboard on grafana.net](https://grafana.net/dashboards/279) that displays all vital metrictank stats.
This dashboard originates from the metrictank repository, and in fact, is also automatically imported into the stack when you spin it up.
Go to the dashboard selector up on top and select "Metrictank". You should see something like the below.
(if no data shows up, you may have opened it too soon after starting the stack. Usually data starts showing a minute after grafana has started. Refresh the page if needed)

![Dashboard screenshot](https://raw.githubusercontent.com/grafana/metrictank/master/docs/assets/dashboard-screenshot.png)


You can also import dashboards from [grafana.com](http://grafana.com/dashboards/)
For example the [statsdaemon dashboard](https://grafana.net/dashboards/297) which shows you metrics about the metrics received by statsdaemon.  Very meta.
To import, go to the dashboard dropdown -> import dashboard -> and paste in `https://grafana.net/dashboards/279` into the Grafana.net url field.
If it asks which datasource to use, enter `metrictank`.


Now you can send in more data [using the plaintext protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html) or using any
of the plethora of [tools that can send data in carbon format](http://graphite.readthedocs.io/en/latest/tools.html)
, create dashboards (or import them from [grafana.net](https://grafana.net)), etc.

[fakemetrics](https://github.com/raintank/fakemetrics) is a handy tool to generate a data stream.
E.g. run `fakemetrics feed --carbon-addr localhost:2003` and the stats will show up under `some.id.of.a.metric.*`

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

## Other docker stacks

The metrictank repository holds other stack configurations as well (for testing, benchmarking, etc). See [devdocs/docker](../devdocs/docker.md) for more info.
