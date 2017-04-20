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

Take the file from `go/src/github.com/raintank/metrictank/metrictank-sample.ini`, put it in `/etc/metrictank/metrictank.ini` and make any changes.

## Run it!

```
/root/go/bin/metrictank
```

Note that metrictank simply logs to stdout, and not to a file.

# Install graphite

This will be needed to query metrictank.

Install Graphite via your prefered method as detailed at http://graphite.readthedocs.io/en/latest/install.html
(We hope to provide Debian and Ubuntu packages in the near future.)

Configure graphite with the following settings in local_settings.py
```
CLUSTER_SERVERS = ['localhost:6060'] # update to match the host:port metrictank is running on.
REMOTE_EXCLUDE_LOCAL = False
USE_WORKER_POOL = True
POOL_WORKERS_PER_BACKEND = 8
POOL_WORKERS = 1
REMOTE_FIND_TIMEOUT = 30.0
REMOTE_FETCH_TIMEOUT = 60.0
REMOTE_RETRY_DELAY = 60.0
MAX_FETCH_RETRIES = 2
FIND_CACHE_DURATION = 300
REMOTE_STORE_USE_POST = True
REMOTE_STORE_FORWARD_HEADERS = ["x-org-id"]
REMOTE_PREFETCH_DATA = True
STORAGE_FINDERS = ()
```
