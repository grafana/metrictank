#!/bin/bash

for i in $(grep 'func main' */*.go | sed 's#/.*##'); do
  cd $i
  echo ">>> $i"
  go build
  cd ..
done
