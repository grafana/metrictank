#!/bin/bash
for endpoint in "$@"; do
  while true; do
    echo "waiting for $endpoint to become up..."
    IFS=: read host port <<< $endpoint
    nc -z $host $port && echo "$endpoint is up!" && break
    sleep 1
  done
done

