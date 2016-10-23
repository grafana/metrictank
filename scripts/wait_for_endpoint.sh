#!/bin/sh

log () {
	echo "$(date +'%Y/%m/%d %H:%M:%S') $@"
}


WAIT_TIMEOUT=${WAIT_TIMEOUT:-10}
CONN_HOLD=${CONN_HOLD:-3}

# test if we're using busybox for timeout
timeout_path=$(readlink $(which timeout))
if [ "$timeout_path" = "" ]
then
  _using_busybox=0
else
  timeout_exec=$(basename $timeout_path)
  if [ "$timeout_exec" = "busybox" ]
  then
    _using_busybox=1
  else
    _using_busybox=0
  fi
fi

if [ $_using_busybox -eq 1 ]
then
  log "using busybox"
else
  log "not using busybox"
fi

for endpoint in $(echo $WAIT_HOSTS | tr "," "\n")
do
  host=${endpoint%:*}
  port=${endpoint#*:}

  _start_time=$(date +%s)
  while true
  do
    _now=$(date +%s)
    _run_time=$(( $_now - $_start_time ))
    if [ $_run_time -gt $WAIT_TIMEOUT ]
    then
        log "timed out waiting for $endpoint"
        exit 1
    fi
    log "waiting for $endpoint to become up..."

    # connect and see if connection stays up.
    # docker-proxy can listen to ports before the actual service is up,
    # in which case it will accept and then close the connection again.
    # this checks not only if the connect succeeds, but also if the 
    # connection stays up for $CONN_HOLD seconds.
    if [ $_using_busybox -eq 1 ]
    then
      timeout -t $CONN_HOLD busybox nc $host $port
      retval=$?

      # busybox-timeout on alpine returns 0 on timeout
      expected=0
    else
      timeout $CONN_HOLD nc $host $port
      retval=$?

      # coreutils-timeout returns 124 if it had to kill the slow command
      expected=124
    fi

    if [ $retval -eq $expected ]
    then
      log "$endpoint is up. maintained connection for $CONN_HOLD seconds!"
      break
    else
      log "returned value $retval, expecting $expected"
    fi

    sleep 1
  done
done

exec $@
