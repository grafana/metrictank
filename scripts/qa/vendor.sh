#!/bin/bash
set -e

# checks whether vendor directory is healthy

export GO111MODULE=off
go get -u github.com/golang/dep/cmd/dep

dep version
dep status
dep check
