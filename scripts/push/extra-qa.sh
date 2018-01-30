#!/bin/bash

log () {
        echo "$(date +'%Y/%m/%d %H:%M:%S') $@"
}

VERSION=`git describe --abbrev=7`

# try returns if did not succeed and is retryable
# exits in all other cases
try () {
	log "trying to push $VERSION to qa server"
	res=$(curl -q https://mt-qa.grafana.net/try -d secret=$MTQA_SECRET -d version=$VERSION)
	echo $res
	if grep -q '^OK$' <<< "$res"; then
		exit 0
	fi
	if ! grep -q 'already has an active deployment, please try again' <<< "$res"; then
		echo "error occurred and not sure how to handle"
		exit 2
	fi
}

n=0
until [ $n -ge 6 ]
do
      try
      n=$[$n+1]
      sleep 10
done

log "giving up"
exit 2
