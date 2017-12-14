#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

tmp=$(mktemp)

echo "checking tools"
if [ ! -r ../docs/tools.md ]; then
	echo "docs/tools.md not a readable file?"
	exit 2
fi
./tools-to-doc.sh > $tmp
diff ../docs/tools.md $tmp
ret=$?
rm $tmp
[ $ret -gt 0 ] && exit $ret

echo "checking configs"
if [ ! -r ../docs/config.md ]; then
	echo "docs/config.md not a readable file?"
	exit 2
fi
./config-to-doc.sh > $tmp
diff ../docs/config.md $tmp
ret=$?
rm $tmp
exit $ret

# metrics2docs .> docs/metrics.md this doesn't work very well yet
