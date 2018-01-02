#!/bin/bash
set -x
BASE=$(dirname $0) # points to scripts directory
CODE_DIR=$(readlink -e "$BASE/../") # project root
BUILD_ROOT=$CODE_DIR/build # should have all binaries already inside
BUILD_PKG=$CODE_DIR/build_pkg # will place packages here
BUILD_TMP=$CODE_DIR/build_tmp # used for temporary data used to construct the packages
mkdir $BUILD_TMP
mkdir $BUILD_PKG

sudo apt-get update # otherwise the below install command doesn't work
sudo apt-get install rpm # to be able to make rpms

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)

## debian wheezy
BUILD=${BUILD_TMP}/sysvinit
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/metrictank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD_PKG}/sysvinit/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-init ${BASE}/config/sysvinit/init.d/metrictank \
  --deb-default ${BASE}/config/sysvinit/default/metrictank \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  --config-files /etc/metrictank/ \
  -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 14.04
BUILD=${BUILD_TMP}/upstart

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/metrictank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD_PKG}/upstart/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --deb-upstart ${BASE}/config/upstart/metrictank \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  --config-files /etc/metrictank/ \
  -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 16.04, Debian 8, CentOS 7
BUILD=${BUILD_TMP}/systemd

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc/metrictank
mkdir -p ${BUILD}/var/run/metrictank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/systemd/metrictank.service $BUILD/lib/systemd/system/
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD_PKG}/systemd/metrictank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --config-files /etc/metrictank/ \
  -m "Raintank Inc. <hello@grafana.com>" --vendor "grafana.com" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

BUILD=${BUILD_TMP}/systemd-centos7

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc/metrictank
mkdir -p ${BUILD}/var/run/metrictank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/systemd/metrictank.service $BUILD/lib/systemd/system/
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD_PKG}/systemd-centos7/metrictank-${VERSION}.el7.${ARCH}.rpm"
fpm -s dir -t rpm \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --config-files /etc/metrictank/ \
  -m "Raintank Inc. <hello@grafana.com>" --vendor "grafana.com" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

## CentOS 6
BUILD=${BUILD_TMP}/upstart-0.6.5

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/metrictank

cp ${BASE}/config/metrictank-package.ini ${BUILD}/etc/metrictank/metrictank.ini
cp ${BASE}/config/storage-schemas.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/storage-aggregation.conf ${BUILD}/etc/metrictank/
cp ${BASE}/config/upstart-0.6.5/metrictank.conf $BUILD/etc/init
cp ${BUILD_ROOT}/{metrictank,mt-*} ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD_PKG}/upstart-0.6.5/metrictank-${VERSION}.el6.${ARCH}.rpm"
fpm -s dir -t rpm \
  -v ${VERSION} -n metrictank -a ${ARCH} --description "metrictank, the gorilla-inspired timeseries database backend for graphite" \
  --replaces metric-tank --provides metric-tank \
  --conflicts metric-tank \
  --config-files /etc/metrictank/ \
  -C ${BUILD} -p ${PACKAGE_NAME} .
