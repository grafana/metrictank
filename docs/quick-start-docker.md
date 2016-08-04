# Quick start using docker

[Docker](docker.io) is a toolkit and a daemon which makes running foreign applications convenient, via containers.
This tutorial will help you run metrictank, its dependencies, and grafana for dashboarding, with minimal hassle.

[docker installation instructions](https://www.docker.com/products/overview)
You will also need to install [docker-compose](https://docs.docker.com/compose/)

First go into the `docker` dir of this project.
You can bring up the stack like so:

```
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

Once the stack is up, metrictank should be running on port 6060:

```
$ curl http://localhost:6060
OK
$ curl http://localhost:6060/cluster
{"instance":"default","primary":true,"lastChange":"2016-08-02T17:12:25.339785926Z"}
```

Then, in your browser, open Grafana which is at `http://localhost:3000` and log in as `admin:admin`
In the menu upper left, hit `Data Sources` and then the `add data source` button.
Add a new data source of name `metrictank`, type `Graphite`, uri `http://localhost:8080` and access mode `direct` (not `proxy`).

When you hit save, Grafana should succeed in talking to the data source.

![Add data source screenshot](https://raw.githubusercontent.com/raintank/metrictank/master/docs/img/add-datasource-docker.png)

Now let's see some data.  If you go to `Dashboards`, `New` and add a new graph panel, you can see that for the `metrictank` there
will already be a bunch of data: 

```
while true; do echo -n "example:$((RANDOM % 100))|c" | nc -w 1 -u localhost 8125; done
```

TODO: send metrics to MT carbon port, validate MT own metrics show up, import MT own dashboard

Finally, you can tear down the entire stack like so:
```
docker-compose stop
```
