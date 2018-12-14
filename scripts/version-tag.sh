#!/bin/bash

version=$(git describe --abbrev=7)
version_raw=$(git describe --abbrev=7 | sed 's/^v//')
gitroot=$(git rev-parse --show-toplevel)

# only tag as latest if:
# * we're in master branch
# * the version string has no hyphen in it (e.g. our git commit equates to a tag)
# * the working tree is not dirty
tag=master
if grep -q master "$gitroot/.git/HEAD" && [[ $version != *-* && -z $(git status --porcelain) ]]; then
	tag=latest
fi
