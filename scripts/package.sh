#!/bin/bash
set -x
BASE=$(dirname $0)
CODE_DIR=$(readlink -e "$BASE/../")

sudo apt-get install rpm # to be able to make rpms

BUILD_ROOT=$CODE_DIR/build

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)

## debian wheezy
BUILD=${BUILD_ROOT}/sysvinit
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/raintank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-init ${BASE}/config/sysvinit/init.d/metrictank \
  --deb-default ${BASE}/config/sysvinit/default/metrictank \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 14.04
BUILD=${BUILD_ROOT}/upstart

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/raintank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/metrictank ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-upstart ${BASE}/config/upstart/metrictank \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 16.04, Debian 8, CentOS 7
BUILD=${BUILD_ROOT}/systemd

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc/raintank
mkdir -p ${BUILD}/var/run/raintank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/raintank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/metrictank ${BUILD}/usr/sbin/
cp ${BASE}/config/systemd/metrictank.service $BUILD/lib/systemd/system/

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --config-files /etc/raintank/ \
  -m "Raintank Inc. <hello@raintank.io>" --vendor "raintank.io" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

BUILD=${BUILD_ROOT}/systemd-centos7

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc/raintank
mkdir -p ${BUILD}/var/run/raintank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/raintank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/metrictank ${BUILD}/usr/sbin/
cp ${BASE}/config/systemd/metrictank.service $BUILD/lib/systemd/system/

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}.el7.${ARCH}.rpm"
fpm -s dir -t rpm \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --config-files /etc/raintank/ \
  -m "Raintank Inc. <hello@raintank.io>" --vendor "raintank.io" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

## CentOS 6
BUILD=${BUILD_ROOT}/upstart-0.6.5

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/raintank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/raintank/
cp ${BUILD_ROOT}/metrictank ${BUILD}/usr/sbin/
cp ${BASE}/config/upstart-0.6.5/metrictank.conf $BUILD/etc/init

PACKAGE_NAME="${BUILD}/metrictank-${VERSION}.el6.${ARCH}.rpm"
fpm -s dir -t rpm \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  -C ${BUILD} -p ${PACKAGE_NAME} .
