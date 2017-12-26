#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../..
if [ -z ${PACKAGECLOUD_REPO} ] ; then
  echo "The environment variable PACKAGECLOUD_REPO must be set."
  exit 1
fi

# Ubuntu 14.04, 16.04, debian 7 (wheezy) & debian 8 (jessie)
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/trusty build/upstart/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/xenial build/systemd/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/debian/wheezy build/sysvinit/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/debian/jessie build/systemd/*.deb

# CentOS 6
package_cloud push ${PACKAGECLOUD_REPO}/el/6 build/upstart-0.6.5/*rpm

# CentOS 7
package_cloud push ${PACKAGECLOUD_REPO}/el/7 build/systemd-centos7/*rpm
