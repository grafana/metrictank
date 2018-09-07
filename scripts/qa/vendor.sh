#!/bin/bash

# checks whether vendor directory is healthy

go get -u github.com/golang/dep/cmd/dep

dep version
dep status
dep check
