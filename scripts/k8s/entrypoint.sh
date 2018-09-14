#!/bin/sh
set -x

export MT_SWIM_BIND_ADDR="${POD_IP}:7946"
export MT_CLUSTER_MODE
export MT_PROFTRIGGER_PATH=${MT_PROFTRIGGER_PATH:-/var/metrictank/$HOSTNAME}
if ! mkdir -p $MT_PROFTRIGGER_PATH; then
	echo "failed to create dir '$MT_PROFTRIGGER_PATH'"
	exit 1
fi

# set any GO environment variables (which we allow to be passed in as MT_GO<foo>

while read var val; do
	export $var=$val
done < <(env | sed -n '/^MT_GO/s/=/ /p' | sed 's/MT_//')

# set offsets
if [ x"$MT_KAFKA_MDM_IN_OFFSET" = "xauto" ]; then
  export MT_KAFKA_MDM_IN_OFFSET=$(/getOffset.py $MT_KAFKA_MDM_IN_TOPICS)
fi

if [ x"$MT_KAFKA_CLUSTER_OFFSET" = "xauto" ]; then
  export MT_KAFKA_CLUSTER_OFFSET=$(/getOffset.py $MT_KAFKA_CLUSTER_TOPIC)
fi

# set cluster PEERs
if [ ! -z "$LABEL_SELECTOR" ]; then
	export MT_CLUSTER_MODE="multi"
	POD_NAMESPACE=${POD_NAMESPACE:-default}
	SERVICE_NAME=${SERVICE_NAME:-metrictank}
	if [ ! -z $KUBERNETES_SERVICE_PORT_HTTP ]; then
		PROTO="http"
		PORT=$KUBERNETES_SERVICE_PORT_HTTP
		HOST=$KUBERNETES_SERVICE_HOST
	fi
	if [ ! -z $KUBERNETES_SERVICE_PORT_HTTPS ]; then
		PROTO="https"
		PORT=$KUBERNETES_SERVICE_PORT_HTTPS
		HOST=$KUBERNETES_SERVICE_HOST
	fi

	if [ -z $HOST ]; then
		echo "ERROR: No kubernetes API host found."
		exit 1
	fi

	if [ ! -d /var/run/secrets/kubernetes.io/serviceaccount ]; then
		echo "ERROR: serviceAccount volume not mounted."
		exit 1
	fi

	echo "querying service $SERVICE_NAME for other metrictank nodes"
	CA="--cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	AUTH="Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"

	_PODS=$(curl -s $CA -H "$AUTH" $PROTO://${HOST}:${PORT}/api/v1/namespaces/${POD_NAMESPACE}/pods?labelSelector=$LABEL_SELECTOR|jq .items[].status.podIP|sed -e "s/\"//g")
	LIST=
	for server in $_PODS; do
	 LIST="${LIST}$server,"
	done
	export MT_CLUSTER_PEERS=$(echo $LIST | sed 's/,$//')
fi

if [ -z "$(echo $@)" ]; then
	exec /usr/bin/metrictank -config=/etc/metrictank/metrictank.ini
else
	exec $@
fi
