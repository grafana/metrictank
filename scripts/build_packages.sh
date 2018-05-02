#!/bin/bash
set -e
set -x
BASE=$(dirname ${0}) # points to scripts directory
CODE_DIR=$(readlink -e "${BASE}/../") # project root
BUILD_PKG=${CODE_DIR}/build_pkg # will place packages here

rm -rf ${BUILD_PKG}
mkdir ${BUILD_PKG}

ARCH="$(uname -m)"
VERSION=$(git describe --long --abbrev=7)


## debian wheezy ##

BUILD_NAME="sysvinit"
PKG_NAME="metrictank-${VERSION}_${ARCH}.deb"
PKG="/tmp/${PKG_NAME}"
docker build --build-arg "PKG=${PKG}" --build-arg "ARCH=${ARCH}" --build-arg "VERSION=${VERSION}" -f scripts/build_packages/${BUILD_NAME}/Dockerfile -t ${BUILD_NAME}:build .
mkdir -p ${BUILD_PKG}/${BUILD_NAME}
docker run ${BUILD_NAME}:build cat ${PKG} > ${BUILD_PKG}/${BUILD_NAME}/${PKG_NAME}

## ubuntu 14.04 ##

BUILD_NAME="upstart"
PKG_NAME="metrictank-${VERSION}_${ARCH}.deb"
PKG="/tmp/${PKG_NAME}"
docker build --build-arg "PKG=${PKG}" --build-arg "ARCH=${ARCH}" --build-arg "VERSION=${VERSION}" -f scripts/build_packages/${BUILD_NAME}/Dockerfile -t ${BUILD_NAME}:build .
mkdir -p ${BUILD_PKG}/${BUILD_NAME}
docker run ${BUILD_NAME}:build cat ${PKG} > ${BUILD_PKG}/${BUILD_NAME}/${PKG_NAME}

## ubuntu 16.04, Debian 8, CentOS 7 ##

BUILD_NAME="systemd"
PKG_NAME="metrictank-${VERSION}_${ARCH}.deb"
PKG="/tmp/${PKG_NAME}"
docker build --build-arg "PKG=${PKG}" --build-arg "ARCH=${ARCH}" --build-arg "VERSION=${VERSION}" -f scripts/build_packages/${BUILD_NAME}/Dockerfile -t ${BUILD_NAME}:build .
mkdir -p ${BUILD_PKG}/${BUILD_NAME}
docker run ${BUILD_NAME}:build cat ${PKG} > ${BUILD_PKG}/${BUILD_NAME}/${PKG_NAME}

## centos 7 ##

BUILD_NAME="systemd-centos7"
PKG_NAME="metrictank-${VERSION}_${ARCH}.rpm"
PKG="/tmp/${PKG_NAME}"
docker build --build-arg "PKG=${PKG}" --build-arg "ARCH=${ARCH}" --build-arg "VERSION=${VERSION}" -f scripts/build_packages/${BUILD_NAME}/Dockerfile -t ${BUILD_NAME}:build .
mkdir -p ${BUILD_PKG}/${BUILD_NAME}
docker run ${BUILD_NAME}:build cat ${PKG} > ${BUILD_PKG}/${BUILD_NAME}/${PKG_NAME}

## CentOS 6 ##

BUILD_NAME="upstart-0.6.5"
PKG_NAME="metrictank-${VERSION}_${ARCH}.rpm"
PKG="/tmp/${PKG_NAME}"
docker build --build-arg "PKG=${PKG}" --build-arg "ARCH=${ARCH}" --build-arg "VERSION=${VERSION}" -f scripts/build_packages/${BUILD_NAME}/Dockerfile -t ${BUILD_NAME}:build .
mkdir -p ${BUILD_PKG}/${BUILD_NAME}
docker run ${BUILD_NAME}:build cat ${PKG} > ${BUILD_PKG}/${BUILD_NAME}/${PKG_NAME}
