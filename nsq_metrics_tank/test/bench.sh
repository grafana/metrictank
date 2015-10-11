#!/bin/bash
echo "run this first: ./env-load -auth admin:admin -orgs 100 load"

echo "creating list of all metrics you should have after an env-load with 100 orgs, 4 endpoints each, using dev-stack with 1 standard collector"


fulllist=$(mktemp)
for org in {1..100}; do
  for endp in {1..4}; do
    cat env-load-metrics-patterns.txt | sed -e "s#\$org#$org#" -e "s#\$endp#$endp#" >> $fulllist
  done
done

echo "list is at $fulllist -- it is $(wc -l $fulllist) lines long"

echo "> min diversity"
head -n 1 $fulllist | sed 's#^#GET http://localhost:6063/get?render=#' | vegeta attack -rate 2000 | vegeta report

echo "> max diversity"
sed 's#^#GET http://localhost:6063/get?render=#' $fulllist | vegeta attack -rate 2000 | vegeta report
