#!/bin/bash

NAME=raintank-metric
VERSION="$(${GOPATH}/bin/${NAME} -v | cut -f3 -d' ')"
BUILD="$(pwd)/${NAME}-${VERSION}"
ARCH="$(uname -m)"
PACKAGE_NAME="artifacts/${NAME}-VERSION-ITERATION_ARCH.deb"

mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc/${NAME}

cp ${GOPATH}/bin/${NAME} ${BUILD}/usr/bin/

fpm -s dir -t deb \
  -v ${VERSION} -n ${NAME} -a ${ARCH} --iteration 1ubuntu1 --description "Raintank Metric" \
  -C ${BUILD} -p ${PACKAGE_NAME} .
