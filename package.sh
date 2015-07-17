#!/bin/bash

GOBIN=`pwd`
NAME=raintank-metric
VERSION="$(./${NAME} -v | cut -f3 -d' ')"
BUILD="$(pwd)/${NAME}-${VERSION}"
ARCH="$(uname -m)"
PACKAGE_NAME="artifacts/${NAME}-VERSION-ITERATION_ARCH.deb"

mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc

cp ${NAME} ${BUILD}/usr/bin/
cp -r packaging/ubuntu/trusty/fs/etc/raintank ${BUILD}/etc/

fpm -s dir -t deb \
  -v ${VERSION} -n ${NAME} -a ${ARCH} --iteration 1ubuntu1 --description "Raintank Metric" \
  --deb-upstart packaging/ubuntu/trusty/fs/etc/init/raintank-metric.conf \
  -C ${BUILD} -p ${PACKAGE_NAME} .
