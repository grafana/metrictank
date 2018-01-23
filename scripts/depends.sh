#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

which bundler &>/dev/null || sudo gem install bundler
bundle install
