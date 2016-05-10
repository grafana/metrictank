#!/bin/bash
GIT_HASH=$(git rev-parse HEAD)

for i in $(grep 'func main' */*.go | sed 's#/.*##'); do
  cd $i
  echo ">>> $i"
  go build -ldflags "-X main.GitHash=$GIT_HASH"
  cd ..
done
