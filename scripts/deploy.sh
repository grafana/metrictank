#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ls -l  ${DIR}/../build/*.deb
if [ -z ${PACKAGECLOUD_REPO} ] ; then
  echo "The environment variable PACKAGECLOUD_REPO must be set."
  exit 1
fi

# Only do the ubuntu pkg for now.
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/trusty ${DIR}/../build/upstart/*.deb
package_cloud push ${PACKAGECLOUD_REPO}/ubuntu/xenial ${DIR}/../build/systemd/*.deb