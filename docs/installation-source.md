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
