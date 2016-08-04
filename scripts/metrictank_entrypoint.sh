#!/bin/bash

hosts=$(echo $WAIT_HOSTS | tr "," "\n")


wait_for()
{
	IFS=: read HOST PORT <<< $1

    while :
    do
        (echo > /dev/tcp/$HOST/$PORT) >/dev/null 2>&1
        result=$?
        if [[ $result -eq 0 ]]; then
            break
        fi
        sleep 1
    done
}

for h in $hosts; do
	wait_for $h
done

exec /usr/bin/metrictank $@


