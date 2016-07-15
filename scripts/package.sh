#!/bin/bash
set -x
BASE=$(dirname $0)
CODE_DIR=$(readlink -e "$BASE/../")

BUILD=$CODE_DIR/build

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}_${ARCH}.deb"
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/config/ubuntu/trusty/etc/raintank/metric_tank.ini ${BUILD}/etc/raintank/
cp ${BUILD}/metrictank ${BUILD}/usr/sbin/

fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-upstart ${BASE}/config/ubuntu/trusty/etc/init/metric_tank \
  -C ${BUILD} -p ${PACKAGE_NAME} .

