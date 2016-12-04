#!/bin/bash

echo "waiting for Grafana to start listening..."
while true; do
  netstat -nlp | grep -q ':3000' && break
  sleep 0.5
done
echo "ok grafana is listening"

echo "> adding datasources graphite"

curl -u admin:admin \
  -H "content-type: application/json" \
  'http://localhost:3000/api/datasources' -X POST --data-binary '{"name":"graphite","type":"graphite","url":"http://localhost:8000","access":"direct","isDefault":false}'
echo

echo "> adding datasources metrictank"

curl -u admin:admin \
  -H "content-type: application/json" \
  'http://localhost:3000/api/datasources' -X POST --data-binary '{"name":"metrictank","type":"graphite","url":"http://localhost:8080","access":"direct","isDefault":false}'
echo

for file in dashboards/*; do
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

