VERSION=$(shell git describe --tags --always | sed 's/^v//')

build:
	go build -ldflags "-X github.com/raintank/fakemetrics/cmd.Version=$(VERSION)"

install:
	go install -ldflags "-X github.com/raintank/fakemetrics/cmd.Version=$(VERSION)"
