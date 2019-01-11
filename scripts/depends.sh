#!/bin/bash
set -x
set -e
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

which bundle &>/dev/null || sudo gem install bundler
bundle install
