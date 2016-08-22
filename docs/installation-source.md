# Installation guide: From source

This approach is not recommended, because it just gives you the metrictank binary, no dependencies, 
and no configuration for an init system.
To install dependencies or for more complete guides, see [installation guides](https://github.com/raintank/metrictank/blob/master/docs/installation.md).

## The build environment

Building metrictank requires:
* a [Golang](https://golang.org/) compiler.  We recommend version 1.5 or higher.
* [Git](https://git-scm.com/).

On Centos:

```
yum install go git
export GOPATH=$HOME/go
```

You may want to make the `GOPATH` setting persistent, by putting that export line in your `~/.bashrc`.

## Build metrictank

```
go get github.com/raintank/metrictank
```

Take the file from `go/src/github.com/raintank/metrictank/metrictank-sample.ini`, put it in `/etc/raintank/metrictank.ini` and make any changes.

## Run it!

```
/root/go/bin/metrictank
```

Note that metrictank simply logs to stdout, and not to a file.

# Install graphite-raintank

This will be needed to query metrictank.

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
