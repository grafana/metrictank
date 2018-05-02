#!/bin/bash

set -e
set -x

BASE=$(readlink -e $(dirname ${0})/../..) # points to scripts directory
CODE_DIR=$(readlink -e ${BASE}/..) # project root
BUILD_ROOT=$CODE_DIR/build # should have all binaries already inside
BUILD=$CODE_DIR/build_tmp # used for temporary data used to construct the packages

cd ${CODE_DIR}
VERSION=$(git describe --long --abbrev=7)
ARCH=$(uname -m)

mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/metrictank
mkdir -p $(dirname ${PKG})

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/upstart-0.6.5/metrictank.conf $BUILD/etc/init
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/bin/

fpm -s dir -t rpm \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --depends cyrus-sasl \
  --depends openssl \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  --config-files /etc/metrictank/ \
  -C ${BUILD} -p ${PKG} .
