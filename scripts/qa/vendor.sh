#!/bin/bash

if ! which dep >/dev/null; then
	go get -u github.com/golang/dep/cmd/dep || exit 1
fi

dep version
dep status
dep check
