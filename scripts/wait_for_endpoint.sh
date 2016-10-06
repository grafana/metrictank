#!/bin/sh


WAIT_TIMEOUT=${WAIT_TIMEOUT:-10}

# test if we're using busybox for timeout
timeout_exec=$(readlink `which timeout` | awk -F '/' '{print $NF}')
if [ "${timeout_exec}" = "busybox" ]
then
    BUSYBOX=1
else
    BUSYBOX=0
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
    if [ ${BUSYBOX} -eq 1 ]
    then
      timeout -t 3 nc ${host} ${port}
      retval=${?}
      expected=143
    else
      timeout 3 nc ${host} ${port}
      retval=${?}
      expected=124
    fi

    if [ ${retval} -eq ${expected} ]
    then
      echo "${endpoint} is up!"
      break
    fi

    sleep 1
  done
done

exec $@
