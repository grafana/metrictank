#!/bin/bash

for i in nsq_*; do
  cd $i
  echo ">>> $i"
  go build
  cd ..
done
