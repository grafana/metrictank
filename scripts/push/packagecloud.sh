#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../..
source scripts/version-tag.sh

repo=raintank/testing
[ $tag -eq latest ] && repo= raintank/raintank

# Ubuntu 14.04, 16.04, debian 7 (wheezy) & debian 8 (jessie)
package_cloud push $repo/ubuntu/trusty build_pkg/upstart/*.deb
package_cloud push $repo/ubuntu/xenial build_pkg/systemd/*.deb
package_cloud push $repo/debian/wheezy build_pkg/sysvinit/*.deb
package_cloud push $repo/debian/jessie build_pkg/systemd/*.deb

# CentOS 6
package_cloud push $repo/el/6 build_pkg/upstart-0.6.5/*rpm

# CentOS 7
package_cloud push $repo/el/7 build_pkg/systemd-centos7/*rpm
