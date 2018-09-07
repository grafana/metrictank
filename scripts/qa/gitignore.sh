#!/bin/bash

# this script checks whether we have gitignore rules for all binaries we compile

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../../cmd
file=../.gitignore


declare -a missing
declare -a malformed
declare -a extraneous

ret=0

for bin in *; do
	rule="/cmd/$bin/$bin"
	grep -q "^$rule$" $file || missing=("${missing[@]}" "$rule")
done

while read a b c; do
	if [ -n "$c" ]; then
		malformed=("${malformed[@]}" "/cmd/$a/$b/$c...")
	elif [ "$a" != "$b" ]; then
		malformed=("${malformed[@]}" "/cmd/$a/$b")
	elif [ ! -d "$a" ]; then
		extraneous=("${extraneous[@]}" "/cmd/$a/$b")
	fi
done < <(grep '^/cmd/' $file | sed -e 's#/cmd/##' -e 's#/# #g')

if [ ${#missing[@]} -gt 0 ]; then
	echo "missing the following rules:"
	for rule in "${missing[@]}"; do
		echo "$rule"
	done
	ret=2
fi

if [ ${#malformed[@]} -gt 0 ]; then
	echo "malformed rules:"
	for rule in "${malformed[@]}"; do
		echo "$rule"
	done
	ret=2
fi

if [ ${#extraneous[@]} -gt 0 ]; then
	echo "extraneous rules:"
	for rule in "${extraneous[@]}"; do
		echo "$rule"
	done
	ret=2
fi

sorted=$(mktemp)
LC_ALL=en_US sort $file > $sorted
if ! cmp --silent $file $sorted; then
	echo ".gitignore file needs sorting"
	diff $file $sorted
	rm $sorted
	ret=2
fi

[ $ret -gt 0 ] && exit $ret

echo "all gitignore rules for commands in sync with cmd directory"
