#!/bin/bash
set -x
BASE=$(dirname $0)
CODE_DIR=$(readlink -e "$BASE/../")

BUILD_ROOT=$CODE_DIR/build

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)


## ubuntu 14.04
BUILD=${BUILD_ROOT}/upstart

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/config/metrictank.ini ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/metrictank ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-upstart ${BASE}/config/upstart/metrictank.conf \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  -C ${BUILD} -p ${PACKAGE_NAME} .


## ubuntu 16.04
BUILD=${BUILD_ROOT}/systemd
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc/raintank
mkdir -p ${BUILD}/var/run/raintank

cp ${BASE}/config/metrictank.ini ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/metrictank ${BUILD}/usr/sbin/
cp ${BASE}/config/systemd/metrictank.service $BUILD/lib/systemd/system/

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --config-files /etc/raintank/ \
  -m "Raintank Inc. <hello@raintank.io>" --vendor "raintank.io" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .