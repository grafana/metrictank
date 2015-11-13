#!/bin/bash

for i in nsq_* metric_tank; do
  cd $i
  echo ">>> $i"
  go build
  cd ..
done
