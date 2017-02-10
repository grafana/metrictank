#!/bin/bash

# different docker stacks may have a different arrangement of datasources, for example
# the standard / sample stacks use metrictank + graphite-api for the data "in metrictank" (e.g. fakemetrics, worldping, ..) as well as monitoring data (such as MT's perf stats)
# fancier stacks such as the clustering one have a separate graphite server for monitoring data.
# there needs to be an elegant way to make this seamless when importing dashboards: they should be setup so that they point to the correct datasource.
# we could fix this by having different datasource settings, and in some cases pointing them to the exact same backend (e.g. a 'monitoring' and 'metrictank' datasource that for the standard stack would point to graphite-api backed by metrictank), but that could cause some confusion when using the UI.

# so instead, we use the following convention for dashboards with an __inputs section::

# 1) DS_GRAPHITE -> data in "graphite" for monitoring (could be backed by metrictank in simple setups. We could potentially change this to DS_MONITORING
# 2) DS_GRAPHITENG -> data in metrictank, queried via graphite-api
# [ in the future we may want to add:
# 3) DS_METRICTANK -> querying metrictank directly (very uncommon) ]

# the stacks then declare how they map these to concrete datasources, so that when we import the dashboard for a given stack, we know which datasource to use.

env=$1

WAIT_HOSTS=localhost:3000 ../../scripts/wait_for_endpoint.sh

for file in $env/datasources/*.json; do
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
	      \"value\": \"$(cat $env/datasources/DS_GRAPHITE)\"
            },
            {
              \"name\": \"DS_GRAPHITENG\",
              \"type\": \"datasource\",
              \"pluginId\": \"graphite\",
	      \"value\": \"$(cat $env/datasources/DS_GRAPHITENG)\"
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
