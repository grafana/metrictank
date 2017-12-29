#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

sudo apt install rubygems ruby-dev
sudo gem install bundler
bundle install
