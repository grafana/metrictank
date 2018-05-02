#!/bin/bash

set -e
set -x

BASE=$(readlink -e $(dirname ${0})/../..) # points to scripts directory
CODE_DIR=$(readlink -e ${BASE}/..) # project root
BUILD_ROOT=$CODE_DIR/build # should have all binaries already inside
BUILD=$CODE_DIR/build_tmp # used for temporary data used to construct the packages

cd ${CODE_DIR}

mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc/metrictank
mkdir -p $(dirname ${PKG})

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/bin/

fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --depends libssl1.0.0 \
  --depends libsasl2-2 \
  --deb-init ${BASE}/config/sysvinit/init.d/metrictank \
  --deb-default ${BASE}/config/sysvinit/default/metrictank \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  --config-files /etc/metrictank/ \
  -C ${BUILD} -p ${PKG} .
