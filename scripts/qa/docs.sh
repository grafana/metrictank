#!/bin/bash
# this script checks whether doc files that are auto-generated
# have been updated
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..

tmp=$(mktemp)

echo "checking tools"
if [ ! -r docs/tools.md ]; then
	echo "docs/tools.md not a readable file?"
	exit 2
fi
scripts/dev/tools-to-doc.sh > $tmp
if ! diff docs/tools.md $tmp; then
  echo "docs/tools.md does not match output of scripts/dev/tools-to-doc.sh"
  rm $tmp
  exit 2
fi

echo "checking configs"
if [ ! -r docs/config.md ]; then
	echo "docs/config.md not a readable file?"
	exit 2
fi
scripts/dev/config-to-doc.sh > $tmp
if ! diff docs/config.md $tmp; then
  echo "docs/config.md does not match output of scripts/dev/config-to-doc.sh"
  rm $tmp
  exit 2
fi

echo "checking metrics.md"
export GO111MODULE=off
go get -u github.com/Dieterbe/metrics2docs || exit 2
metrics2docs . > $tmp
diff docs/metrics.md $tmp
ret=$?
rm $tmp
if [ $ret -gt 0 ]; then
  echo "docs/metrics.md does not match output of docs2metrics"
fi
exit $ret
