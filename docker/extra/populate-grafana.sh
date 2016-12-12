#!/bin/bash

basedir=$(dirname "$0")
envsdir=$basedir/..
env="$1"

if [ ! -d $envsdir/docker-$env ]; then
	echo -e "Could not find docker environment $envsdir/docker-$env\n" >&2
	echo -e "Known environments:\n" >&2
	cd $envsdir
	ls -1d docker-* | sed 's#^docker-##' >&2
	exit 1
fi

echo "waiting for Grafana to start listening..."
while true; do
  netstat -nlp | grep -q ':3000' && break
  sleep 0.5
done
echo "ok grafana is listening"

for file in $envsdir/docker-$env/datasources/*; do
  echo "> adding datasources $file"
  curl -u admin:admin -H "content-type: application/json" 'http://localhost:3000/api/datasources' -X POST --data-binary @$file
  echo
done

for file in $basedir/dashboards/*; do
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

