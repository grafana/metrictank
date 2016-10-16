#!/bin/sh


WAIT_TIMEOUT=${WAIT_TIMEOUT:-10}

# test if we're using busybox for timeout
timeout_exec=$(readlink `which timeout` | awk -F '/' '{print $NF}')
if [ "$timeout_exec" = "busybox" ]
then
  echo "using busybox"
  _using_busybox=1
else
  echo "not using busybox"
  _using_busybox=0
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
        echo "timed out waiting for $endpoint"
        exit 1
    fi
    echo "waiting for $endpoint to become up..."

    # connect and see if connection stays up.
    # docker-proxy can listen to ports before the actual service is up,
    # in which case it will accept and then close the connection again.
    # this checks not only if the connect succeeds, but also if the 
    # connection stays up for 3 seconds.
    if [ $_using_busybox -eq 1 ]
    then
      busybox timeout -t 3 busybox nc $host $port
      retval=$?

      # busybox-timeout on alpine returns 0 on timeout
      expected=0
    else
      timeout 3 nc $host $port
      retval=$?

      # coreutils-timeout returns 124 if it had to kill the slow command
      expected=124
    fi

    if [ $retval -eq $expected ]
    then
      echo "$endpoint is up!"
      break
    else
      echo "returned value $retval, expecting $expected"
    fi

    sleep 1
  done
done

exec $@
