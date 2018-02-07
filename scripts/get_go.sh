#!/bin/bash

GO_DEFAULT_VERSION="1.10.1"
if [ -z "$GO_VERSION" ]
then
	GO_VERSION=$GO_DEFAULT_VERSION
fi
GO_DOWNLOAD_LINK="https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz"

cd /usr/local
curl $GO_DOWNLOAD_LINK | tar -xz
