#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

NAME=raintank-metric
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
  #--deb-upstart ${DIR}/config/ubuntu/trusty/etc/init/raintank-metric.conf \
  #-C ${BUILD} -p ${PACKAGE_NAME} .

for VAR in nsq_metrics_to_elasticsearch	nsq_metrics_to_kairos nsq_probe_events_to_elasticsearch metric_tank; do
	NSQ_BUILD="${DIR}/$VAR-${VERSION}"
	NSQ_PACKAGE_NAME="${DIR}/artifacts/${VAR}-VERSION_ITERATION_ARCH.deb"
	mkdir -p ${NSQ_BUILD}/usr/sbin
	mkdir -p ${NSQ_BUILD}/etc/init
	mkdir -p ${NSQ_BUILD}/etc/raintank
	cp ${DIR}/config/ubuntu/trusty/etc/init/${VAR}.conf ${NSQ_BUILD}/etc/init
	cp ${DIR}/config/ubuntu/trusty/etc/raintank/${VAR}.ini ${NSQ_BUILD}/etc/raintank
	cp ${DIR}/artifacts/$VAR ${NSQ_BUILD}/usr/sbin
	fpm -s dir -t deb \
	  -v ${VERSION} -n ${VAR} -a ${ARCH} --iteration $ITERATION --description "Raintank Metric $VAR worker" \
	  --deb-upstart ${DIR}/config/ubuntu/trusty/etc/init/${VAR}.conf \
	  -C ${NSQ_BUILD} -p ${NSQ_PACKAGE_NAME} .
done
