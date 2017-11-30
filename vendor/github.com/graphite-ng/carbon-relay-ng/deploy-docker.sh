#!/bin/bash

VERSION=$(git describe --tags --always | sed 's/^v//')

echo docker push raintank/carbon-relay-ng:$VERSION
docker push raintank/carbon-relay-ng:$VERSION

echo docker push raintank/carbon-relay-ng:latest
docker push raintank/carbon-relay-ng:latest
