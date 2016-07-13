#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

NAME=metrictank
VERSION="0.1.0" # need an automatic way to do this again :-/
BUILD="${DIR}/${NAME}-${VERSION}"
ARCH="$(uname -m)"
PACKAGE_NAME="${DIR}/artifacts/${NAME}-VERSION-ITERATION_ARCH.deb"
GOBIN="${DIR}/.."
ITERATION=`date +%s`ubuntu1
TAG="pkg-${VERSION}-${ITERATION}"

git tag $TAG

mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank
#cp ${DIR}/config/ubuntu/trusty/etc/init/${NAME}.conf ${BUILD}/etc/init
#cp ${DIR}/config/ubuntu/trusty/etc/raintank/* ${BUILD}/etc/raintank

#cp ../${NAME} ${BUILD}/usr/bin/
#cp -r ${DIR}/config/ubuntu/trusty/* ${BUILD}/

#fpm -s dir -t deb \
  #-v ${VERSION} -n ${NAME} -a ${ARCH} --iteration $ITERATION --description "Raintank Metric" \
  #--deb-upstart ${DIR}/config/ubuntu/trusty/etc/init/metrictank.conf \
  #-C ${BUILD} -p ${PACKAGE_NAME} .

BUILD="${DIR}/metrictank-${VERSION}"
PACKAGE_NAME="${DIR}/artifacts/${VAR}-VERSION_ITERATION_ARCH.deb"
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank
cp ${DIR}/config/ubuntu/trusty/etc/raintank/${VAR}.ini ${BUILD}/etc/raintank
cp ${DIR}/artifacts/$VAR ${BUILD}/usr/sbin
fpm -s dir -t deb \
  -v ${VERSION} -n ${VAR} -a ${ARCH} --iteration $ITERATION --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-upstart ${DIR}/config/ubuntu/trusty/etc/init/${VAR} \
  -C ${BUILD} -p ${PACKAGE_NAME} .
