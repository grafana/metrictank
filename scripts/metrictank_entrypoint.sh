#!/bin/sh

hosts=$(echo $WAIT_HOSTS | tr "," "\n")

for endpoint in $hosts; do
  while true; do
    echo "waiting for $endpoint to become up..."
    host=${endpoint#*:}
    port=${endpoint%:*}
    nc -z $host $port && echo "$endpoint is up!" && break
    sleep 1
  done
done

exec /usr/bin/metrictank $@


