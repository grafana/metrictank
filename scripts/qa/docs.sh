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
diff docs/tools.md $tmp
ret=$?
rm $tmp
if [ $ret -gt 0 ]; then 
  echo "docs/tools.md does not match output of scripts/dev/tools-to-doc.sh"
  exit $ret
fi

echo "checking configs"
if [ ! -r docs/config.md ]; then
	echo "docs/config.md not a readable file?"
	exit 2
fi
scripts/dev/config-to-doc.sh > $tmp
diff docs/config.md $tmp
ret=$?
rm $tmp
if [ $ret -gt 0 ]; then
  echo "docs/config.md does not match output of scripts/dev/config-to-doc.sh"
fi
exit $ret

# metrics2docs .> docs/metrics.md this doesn't work very well yet
