# Quick start using Docker

[Docker](docker.io) is a toolkit and a daemon which makes running foreign applications convenient, via containers.
This tutorial will help you run metrictank, its dependencies, and grafana for dashboarding, with minimal hassle.

[docker installation instructions](https://www.docker.com/products/overview)
You will also need to install [docker-compose](https://docs.docker.com/compose/)

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

If you already have something else listen to that port (such as a carbon or grafana server), shut it down, as it will conflict.


You can bring up the stack like so:

```
cd docker
docker-compose up
```

A bunch of text will whiz past on your screen, but you should see

```
metrictank_1     | waiting for cassandra:9042 to become up...
statsdaemon_1    | 2016/08/04 12:31:21 ERROR: dialing metrictank:2003 failed - dial tcp 172.18.0.5:2003: getsockopt: connection refused
metrictank_1     | waiting for cassandra:9042 to become up...
statsdaemon_1    | 2016/08/04 12:31:22 ERROR: dialing metrictank:2003 failed - dial tcp 172.18.0.5:2003: getsockopt: connection refused
```

And a little bit later:

```
metrictank_1       | 2016/08/04 11:28:24 [I] DefCache initialized in 50.40667ms. starting data consumption
metrictank_1       | 2016/08/04 11:28:24 [I] carbon-in: listening on :2003/tcp
metrictank_1       | 2016/08/04 11:28:24 [I] starting listener for metrics and http/debug on :6060
```

Once the stack is up, metrictank should be running on port 6060.  
If you're running Docker engine natively, you can connect using `localhost`. If you're using Docker Toolbox (which runs containers inside a VirtualBox VM), the host will be the IP address returned by `docker-machine ip`.

```
$ curl http://localhost:6060
OK
$ curl http://localhost:6060/cluster
{"instance":"default","primary":true,"lastChange":"2016-08-02T17:12:25.339785926Z"}
```

## Working with Grafana and metrictank

In your browser, open Grafana at `http://localhost:3000` (or your docker-machine address) and log in as `admin:admin`.  
In the upper left Grafana menu, choose `Data Sources`. On the next page, click the `Add data source` button.  
Add a new data source with name `metrictank`, check "default", type `Graphite`, Url `http://localhost:8080` and access mode `direct` (not `proxy`).

When you hit save, Grafana should succeed in talking to the data source.

![Add data source screenshot](https://raw.githubusercontent.com/raintank/metrictank/master/docs/assets/add-datasource-docker.png)

Note: it also works with `proxy` mode but then you have to enter `http://graphite-api:8080` as uri.

Now let's see some data.  If you go to `Dashboards`, `New` and add a new graph panel.
In the metrics tab you should see a bunch of metrics already: 

* data under `stats`: these are metrics coming from metrictank and graphite-api.  
  i.e. they send their own instrumentation into statsd (statsdaemon actually is the version we use here),  
  and statsdaemon sends aggregated metrics into metrictank's carbon port.  Statsdaemon flushes every second.
* statsdaemon's own internal metrics which it sends to metrictank's carbon port.
* after about 5 minutes you'll also have some usage metrics show up under `metrictank`. See
[Usage reporting](https://github.com/raintank/metrictank/blob/master/docs/usage-reporting.md)


Note that metrictank is setup to track every metric on a 1-second granularity.  If you wish to use it for less frequent metrics,
you have to modify the storage-schemas.conf, just like with graphite.

You can also send your own data into metrictank using the carbon input, like so:

```
echo "example.metric 123 $(date +%s)" | nc localhost 2003
```


Now for something neat!  
There is an extensive [dashboard on grafana.net](https://grafana.net/dashboards/279) that displays all vital metrictank stats.

![Dashboard screenshot](https://raw.githubusercontent.com/raintank/metrictank/master/docs/assets/dashboard-screenshot.png)

So go to the dashboard dropdown -> import -> and paste in `https://grafana.net/dashboards/279` into the Grafana.net url field.
It will show a dialog with a choice of which graphite datasource to use, for which you can enter `metrictank`.

You should now have a functioning dashboard showing all metrictank's internal metrics, which it reports via statsdaemon, back into itself.


Feel free to play around more, send in more data, create dashboards (or import them from [grafana.net](https://grafana.net)), etc.

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
