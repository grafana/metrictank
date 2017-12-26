#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
file=$DIR/../../.gitignore

# NOTE: known limitation: does not clean up stale gitignore rules

cd $GOPATH/src/github.com/grafana/metrictank/cmd
declare -a missing
for tool in *; do
	rule="/cmd/$tool/$tool"
	grep -q "^$rule$" $file || missing=("${missing[@]}" "$rule")
done

if [ ${#missing[@]} -gt 0 ]; then
	echo "missing the following rules:"
	for rule in "${missing[@]}"; do
		echo "$rule"
	done
	exit 2
fi

echo "gitignore rules for all tools found"
