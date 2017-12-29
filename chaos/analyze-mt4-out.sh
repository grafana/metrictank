#!/bin/bash
echo "Breakdown of what MT4 logged for requests issued from the isolation test"

echo "started renders"
docker logs dockerchaos_metrictank4_1 2>&1 | grep -c 'Started GET /render.*15s'
echo "completed renders"
docker logs dockerchaos_metrictank4_1 2>&1 | grep -c 'Completed /render.*15s'

echo "renders that completed in a mere µs|ms"
ms=$(docker logs dockerchaos_metrictank4_1 2>&1 | grep 'Completed /render.*15s' | egrep -c 'µs|ms')
echo $ms
echo "renders that completed in <5s"
s=$(docker logs dockerchaos_metrictank4_1 2>&1 | grep 'Completed /render.*15s' | grep -c 'OK.*[0-4]\.[0-9]*s')
echo $s

echo "render 503 not available - within 100ms after cluster timeout of 5s"
good=$(docker logs dockerchaos_metrictank4_1 2>&1 | grep -c 'Completed /render.*15s.*503 Service Unavailable in 5\.0[0-9]*s')
echo $good
echo "render 503 not available - within 100-300ms after cluster timeout of 5s (should be 0)"
ok=$(docker logs dockerchaos_metrictank4_1 2>&1 | grep -c 'Completed /render.*15s.*503 Service Unavailable in 5\.[1-2][0-9]*s')
echo $ok
echo "render 503 not available - other (should be 0)"
all=$(docker logs dockerchaos_metrictank4_1 2>&1 | grep -c 'Completed /render.*15s.*503 Service Unavailable in')
echo $((all-ok-good))

echo "all render calls that may have timed out the client"
docker logs dockerchaos_metrictank4_1 2>&1 | grep 'Completed /render.*15s' | egrep -v 'µs|ms|Completed /render.*503 Service Unavailable in 5\.[0-2][0-9]*s|200 OK.*[0-4]\.[0-9]*s'
