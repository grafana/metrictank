#!/bin/bash
echo "run this first: ./env-load -auth admin:admin -orgs 100 load"

echo "creating list of all metrics you should have after an env-load with 100 orgs, 4 endpoints each, using dev-stack with 1 standard collector"
instances="6063 6064"


fulllist=$(mktemp)
for org in {1..100}; do
  for endp in {1..4}; do
    cat env-load-metrics-patterns.txt | sed -e "s#\$org#$org#" -e "s#\$endp#$endp#" >> $fulllist
  done
done

echo "list is at $fulllist -- it is $(wc -l $fulllist) lines long"

echo "> max diversity"
for i in $instances; do
  echo $i
  sed 's#^#GET http://localhost:'$i'/get?render=#' $fulllist | vegeta attack -rate 2000 | vegeta report
done

echo "> min diversity"
for i in $instances; do
  echo $i
  head -n 1 $fulllist | sed 's#^#GET http://localhost:'$i'/get?render=#' | vegeta attack -rate 2000 | vegeta report
done


