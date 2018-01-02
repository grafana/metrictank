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
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/trusty build_pkg/upstart/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/xenial build_pkg/systemd/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/debian/wheezy build_pkg/sysvinit/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/debian/jessie build_pkg/systemd/*.deb

# CentOS 6
package_cloud push ${PACKAGECLOUD_REPO}/el/6 build_pkg/upstart-0.6.5/*rpm

# CentOS 7
package_cloud push ${PACKAGECLOUD_REPO}/el/7 build_pkg/systemd-centos7/*rpm
