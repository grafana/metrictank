#!/bin/bash

# alerting use case: frequent reads of the same, recent data
./run-alerting.sh &
# exploration case: infrequent, random data, longer timeframe
./run-exploration.sh

sleep 3
echo "alerting:"
cat alerting-out | vegeta report
echo "exploration:"
cat exploration-out | vegeta report
