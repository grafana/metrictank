#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

NAME=raintank-metric
VERSION="$(../${NAME} -v | cut -f3 -d' ')"
BUILD="${DIR}/${NAME}-${VERSION}"
ARCH="$(uname -m)"
PACKAGE_NAME="${DIR}/artifacts/${NAME}-VERSION-ITERATION_ARCH.deb"
GOBIN="${DIR}/.."
ITERATION=`date +%s`ubuntu1

mkdir -p ${BUILD}/usr/bin

cp ../${NAME} ${BUILD}/usr/bin/
cp -r ${DIR}/config/ubuntu/trusty/* ${BUILD}/

fpm -s dir -t deb \
  -v ${VERSION} -n ${NAME} -a ${ARCH} --iteration $ITERATION --description "Raintank Metric" \
  --deb-upstart ${DIR}/config/ubuntu/trusty/etc/init/raintank-metric.conf \
  -C ${BUILD} -p ${PACKAGE_NAME} .
