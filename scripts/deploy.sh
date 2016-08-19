#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ls -l  ${DIR}/../build/*.deb
if [ -z ${PACKAGECLOUD_REPO} ] ; then
  echo "The environment variable PACKAGECLOUD_REPO must be set."
  exit 1
fi

# Ubuntu 14.04, 16.04, debian 7 (wheezy) & debian 8 (jessie)
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/trusty ${DIR}/../build/upstart/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/xenial ${DIR}/../build/systemd/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/debian/wheezy ${DIR}/../build/sysvinit/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/debian/jessie ${DIR}/../build/systemd/*.deb

# CentOS 6
package_cloud push ${PACKAGECLOUD_REPO}/el/6 ${DIR}/../build/upstart-0.6.5/*rpm

# CentOS 7
package_cloud push ${PACKAGECLOUD_REPO}/el/7 ${DIR}/../build/systemd-centos7/*rpm
