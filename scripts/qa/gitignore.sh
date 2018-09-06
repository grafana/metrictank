#!/bin/bash

# this script checks whether we have gitignore rules for all binaries we compile
# NOTE: known limitation: does not clean up stale gitignore rules

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../../cmd
file=../.gitignore


declare -a missing
for bin in *; do
	rule="/cmd/$bin/$bin"
	grep -q "^$rule$" $file || missing=("${missing[@]}" "$rule")
done

if [ ${#missing[@]} -gt 0 ]; then
	echo "missing the following rules:"
	for rule in "${missing[@]}"; do
		echo "$rule"
	done
	exit 2
fi

echo "gitignore rules for all binaries found"
