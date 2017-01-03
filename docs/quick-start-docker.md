# Quick start using Docker

[Docker](docker.io) is a toolkit and a daemon which makes running foreign applications convenient, via containers.
This tutorial will help you run metrictank, its dependencies, and grafana for dashboarding, with minimal hassle.

[docker installation instructions](https://www.docker.com/products/overview)
You will also need to install version >=1.6 of [docker-compose](https://docs.docker.com/compose/)

## Getting the repository

If you already have go installed, you can just: 

```
go get github.com/raintank/metrictank
cd $GOPATH/src/github.com/raintank/metrictank
```

If you don't, you can just use git:

```
git clone https://github.com/raintank/metrictank.git
cd metrictank
```

If you have neither, just [download the zip](https://github.com/raintank/metrictank/archive/master.zip), extract it somewhere and cd into the metrictank directory.

## Bring up the stack

The stack will listen on the following ports:

* 2003 tcp (metrictank's carbon input)
* 3000 tcp (grafana's http port)
* 6060 tcp (metrictank's internal endpoint)
* 8080 tcp (the graphite api query endpoint)
* 8125 udp (statsd endpoint)
* 9042 tcp (cassandra)

If you already have something else listen to that port (such as a carbon or grafana server), shut it down, as it will conflict.


Inside your copy of the repository, you can bring up the stack like so:

```
cd docker/docker-standard
docker-compose up
```

If this gives you the error `service 'version' doesn't have any configuration options`,
your version of `docker-compose` is too old and you need to update to >=1.6.

A bunch of text will whiz past on your screen, but you should see

```
metrictank_1     | waiting for cassandra:9042 to become up...
statsdaemon_1    | 2016/12/04 15:30:21 ERROR: dialing metrictank:2003 failed - dial tcp 172.18.0.5:2003: getsockopt: connection refused. will retry
metrictank_1     | waiting for cassandra:9042 to become up...
statsdaemon_1    | 2016/12/04 15:30:22 ERROR: dialing metrictank:2003 failed - dial tcp 172.18.0.5:2003: getsockopt: connection refused
```

And a little bit later you should see that metrictank starts its listener and statsdaemon connects to metrictank:

```
metrictank_1    | 2016/12/04 15:30:34 [I] metricIndex initialized in 326.379951ms. starting data consumption
metrictank_1    | 2016/12/04 15:30:34 [I] carbon-in: listening on :2003/tcp
metrictank_1    | 2016/12/04 15:30:34 [I] API Listening on: http://:6060/
statsdaemon_1   | 2016/12/04 15:30:35 now connected to metrictank:2003
```

Once the stack is up, metrictank should be running on port 6060.  
If you're running Docker engine natively, you can connect using `localhost`. If you're using Docker Toolbox (which runs containers inside a VirtualBox VM), the host will be the IP address returned by `docker-machine ip`.

```
$ curl http://localhost:6060
OK
$ curl http://localhost:6060/node
{"name":"default","version":"0.5.7-67-g3be576c","primary":true,"primaryChange":"2016-11-15T08:13:12.819435983Z","state":"NodeReady","started":"0001-01-01T00:00:00Z","stateChange":"2016-11-15T08:13:16.591748594Z"}
```

## Working with Grafana and metrictank

In your browser, open Grafana at `http://localhost:3000` (or your docker-machine address) and log in as `admin:admin`.  
In the upper left Grafana menu, choose `Data Sources`. On the next page, click the `Add data source` button.  
Add a new data source with with these settings:

* Name: `metrictank`
* Default: `yes`
* Type: `Graphite`
* Url: `http://localhost:8080`
* Access: `direct` (not `proxy`)

When you hit save, Grafana should succeed in talking to the data source.

![Add data source screenshot](https://raw.githubusercontent.com/raintank/metrictank/master/docs/assets/add-datasource-docker.png)

Note: it also works with `proxy` mode but then you have to enter `http://graphite-api:8080` as uri.

Now let's see some data.  If you go to `Dashboards`, `New` and add a new graph panel.
In the metrics tab you should see a bunch of metrics already in the root hierarchy:

* `service_is_statsdaemon`: statsdaemon's own internal metrics which it sends to metrictank's carbon port.
* `metrictank.stats`: internal stats reported by metrictank
* `metrictank.usage`: usage metrics reported by metrictank.  See
[Usage reporting](https://github.com/raintank/metrictank/blob/master/docs/usage-reporting.md)
It may take a few minutes for the usage metrics to show up.
* `stats`: these are metrics coming from graphite-api, aggregated by statsdaemon and sent back to metrictank every second.


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


Now for something neat!  
There is an extensive [dashboard on grafana.net](https://grafana.net/dashboards/279) that displays all vital metrictank stats.

![Dashboard screenshot](https://raw.githubusercontent.com/raintank/metrictank/master/docs/assets/dashboard-screenshot.png)

So go to the dashboard dropdown -> import -> and paste in `https://grafana.net/dashboards/279` into the Grafana.net url field.
It will show a dialog with a choice of which graphite datasource to use, for which you can enter `metrictank`.

You should now have a functioning dashboard showing all metrictank's internal metrics which it reports into itself.

Another dashboard you can import for instant gratification is the [statsdaemon](https://grafana.net/dashboards/297) dashboard, which shows you
metrics about the metrics.  Very meta.

Now you can send in more data [using the plaintext protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html) or using any
of the plethora of [tools that can send data in carbon format](http://graphite.readthedocs.io/en/latest/tools.html)
, create dashboards (or import them from [grafana.net](https://grafana.net)), etc.

If anything doesn't work, please let us know via a ticket on github or reach out on slack. See
[Community](https://github.com/raintank/metrictank/blob/master/docs/community.md)


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
