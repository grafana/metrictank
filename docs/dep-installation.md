# Dependency Installation

Metrictank has some dependencies. None of them are hard dependencies enforced by the package, because running these services on other machines is a very common configuration, but they'll need to be running somewhere and metrictank won't run without them.

Cassandra, Elasticsearch, and Kafka require Java. We recommend using Oracle Java 8.

## Cassandra

For Debian and Debian based distros (taken from [http://cassandra.apache.org/download/](ttp://cassandra.apache.org/download/)):

* Add this to your `/etc/apt/sources.list`:

```
deb http://www.apache.org/dist/cassandra/debian 30x main 
deb-src http://www.apache.org/dist/cassandra/debian 30x main
```

* Run `gpg --keyserver pgp.mit.edu --recv-keys 0353B12C && gpg --export --armor 0353B12C | sudo apt-key add -` to add the GPG key.

* Run `sudo apt-get update && sudo apt-get install cassandra cassandra-tools`

For RHEL-based distros (taken from http://docs.datastax.com/en/cassandra/3.x/cassandra/install/installRHEL.html):

* Add the DataStax Distribution of Apache Cassandra 3.x repository to the /etc/yum.repos.d/datastax.repo:

```
[datastax-ddc] 
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/datastax-ddc/3.1
enabled = 1
gpgcheck = 0
```

* Run `sudo yum install datastax-ddc`

The defaults for cassandra are fine, especially for testing metrictank out.

## Elasticsearch

For Debian and Debian based distros (taken from https://www.elastic.co/guide/en/elasticsearch/reference/2.3/setup-repositories.html):

* Install the GPG key with `wget -qO - https://packages.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -`

* Save the repository definition to /etc/apt/sources.list.d/elasticsearch-2.x.list:

`echo "deb https://packages.elastic.co/elasticsearch/2.x/debian stable main" | sudo tee -a /etc/apt/sources.list.d/elasticsearch-2.x.list`

* Install elasticsearch with `sudo apt-get install apt-transport-https && sudo apt-get update && sudo apt-get install elasticsearch`

For RHEL based distros (taken from the same URL as above):

* Install the GPG key with `rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch`

* Add the following in your /etc/yum.repos.d/ directory in a file with a .repo suffix, for example elasticsearch.repo

```
[elasticsearch-2.x]
name=Elasticsearch repository for 2.x packages
baseurl=https://packages.elastic.co/elasticsearch/2.x/centos
gpgcheck=1
gpgkey=https://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1
```

* Install elasticsearch with `yum install elasticsearch`

## Kafka

Optional, but recommended.

Kafka requires Zookeeper, so set that up first. (Taken from https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html)

* Download zookeeper. Find a mirror at http://www.apache.org/dyn/closer.cgi/zookeeper/, pick a stable zookeeper, and download it to your server.

* Unpack zookeeper. For this guide we'll install it in `/opt`.

```
cd /opt
tar -zxvf /path/to/zookeeper-3.4.8.tar.gz
ln -s /opt/zookeeper-3.4.8 /opt/zookeeper
mkdir /var/lib/zookeeper
```

* Make a config file for zookeeper in `/opt/zookeeper/conf/zoo.cfg`:

```
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
```

* Start zookeeper: `/opt/zookeeper/bin/zkServer.sh start`

Now we can setup kafka. (Taken from https://kafka.apache.org/documentation.html#quickstart) 

* Download kafka. Find a mirror at https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz, and download kafka to your server.

* Unpack kafka. Like zookeeper, we'll do so in `/opt`.

```
cd /opt
tar -zxvf /path/to/kafka_2.11-0.10.0.1.tgz
ln -s /opt/kafka_2.11-0.10.0.1 /opt/kafka
```

* Start kafka: `/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties`

## graphite-metrictank

The easiest way to install graphite-metrictank (along with our graphite-api fork) is [by adding our packagecloud repo](https://packagecloud.io/raintank/raintank/install) and installing from there.

If, for whatever reason, you can't do that:

* Install the build dependencies. 
  * Under debian based distros, run `apt-get -y install python python-pip build-essential python-dev libffi-dev libcairo2-dev git` as root. 
  * For CentOS and other rpm-based distros, run `yum -y install python-setuptools python-devel gcc gcc-c++ make openssl-devel libffi-devel cairo-devel git; easy_install pip`.
  * If neither of these instructions are relevant to you, figure out how your distribution or operating system refers to the above packages and install them.

* Install `virtualenv`, if desired: `pip install virtualenv virtualenv-tools`

* If you are installing graphite using `virtualenv`:
  * `virtualenv /usr/share/python/graphite`
  * Run all of the pip commands below as `/usr/share/python/graphite/bin/pip`.

* Run these commands to install:
  * `git clone https://github.com/raintank/graphite-metrictank.git`
  * `pip install git+https://github.com/raintank/graphite-api.git`
  * `pip install gunicorn==18.0`
  * `pip install /path/to/graphite-metrictank`
  * `pip install eventlet`
  * `pip install git+https://github.com/woodsaj/pystatsd.git`
  * `pip install Flask-Cache`
  * `pip install python-memcached`
  * `pip install blist`
  * `find /usr/share/python/graphite ! -perm -a+r -exec chmod a+r {} \;`
  * `cd /usr/share/python/graphite`
  * `virtualenv-tools --update-path /usr/share/python/graphite`
  * `mkdir -p /var/log/graphite`

The easiest way to run graphite-api + graphite-metrictank when you've installed it from source is to find the appropriate startup script in the `pkg/` directory in the graphite-metrictank repo, the defaults file at `pkg/common/default/graphite-metrictank`, and the `graphite-metrictank.yaml` and `gunicorn_conf.py` config files in `pkg/common/graphite-metrictank`. **NB:** If you do not use `virtualenv` to install graphite-api and graphite-metrictank, you will need to modify the startup script to point at the system python and the gunicorn you installed (which will probably be at `/usr/local/bin/gunicorn`).
