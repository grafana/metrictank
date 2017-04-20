#!/bin/bash

env=$1

WAIT_HOSTS=localhost:3000 WAIT_TIMEOUT=120 ../../scripts/wait_for_endpoint.sh

for file in $env/datasources/*; do
  echo "> adding datasources $file"
  curl -u admin:admin -H "content-type: application/json" 'http://localhost:3000/api/datasources' -X POST --data-binary @$file
  echo
done

for file in $env/../../dashboard.json $env/../extra/dashboards/*; do
  if grep -q "__inputs" $file; then
    echo "> importing dashboard $file"
    curl -u admin:admin \
      -H "content-type: application/json" \
      'http://localhost:3000/api/dashboards/import' -X POST -d "
        {
          \"dashboard\":
             $(cat $file),
          \"overwrite\": true,
          \"inputs\": [
            {
              \"name\": \"DS_GRAPHITE\",
              \"type\": \"datasource\",
              \"pluginId\": \"graphite\",
              \"value\": \"graphite\"
            }
          ]
        }"
  else
    echo "> adding dashboard $file"
    curl -u admin:admin \
      -H "content-type: application/json" \
      'http://localhost:3000/api/dashboards/db' -X POST -d "{\"dashboard\": $(cat $file)}"
  fi
  echo
  echo
done
