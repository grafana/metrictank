#!/bin/bash

set -e
set -x

MAJOR_VERSION="2.5"
MINOR_VERSION="${MAJOR_VERSION}.1"

cd /tmp
yum install -y wget
wget http://ftp.ruby-lang.org/pub/ruby/${MAJOR_VERSION}/ruby-${MINOR_VERSION}.tar.gz
tar -xvzf /tmp/ruby-${MINOR_VERSION}.tar.gz
cd /tmp/ruby-${MINOR_VERSION}
./configure
make
make install

cd ${OLD_PWD}
