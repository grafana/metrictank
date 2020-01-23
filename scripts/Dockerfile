#############################
# unpublished builder image #
#############################
FROM golang:1.11.4-alpine AS gobase
RUN apk add --no-cache git bash

# Compile metrictank
ENV MT_SRC_DIR $GOPATH/src/github.com/grafana/metrictank
RUN mkdir -p $MT_SRC_DIR
ADD . $MT_SRC_DIR
RUN $MT_SRC_DIR/scripts/build.sh
RUN cp -r $MT_SRC_DIR/build /tmp/build

####################
# mt-gateway image #
####################
FROM alpine:3.10.0 AS mt-gateway

RUN mkdir -p /etc/metrictank
COPY scripts/config/storage-schemas.conf /etc/metrictank/storage-schemas.conf
COPY --from=gobase /tmp/build/mt-gateway /usr/bin/

EXPOSE 6059

ENTRYPOINT ["/usr/bin/mt-gateway"]

#########################
# main metrictank image #
#########################
FROM alpine:3.10.0
MAINTAINER Dieter Plaetinck dieter@grafana.com

RUN apk add -U tzdata

RUN mkdir -p /etc/metrictank /usr/share/metrictank/examples
COPY scripts/config/metrictank-docker.ini /etc/metrictank/metrictank.ini
COPY scripts/config/index-rules.conf /etc/metrictank/index-rules.conf
COPY scripts/config/storage-schemas.conf /etc/metrictank/storage-schemas.conf
COPY scripts/config/storage-aggregation.conf /etc/metrictank/storage-aggregation.conf
COPY scripts/config/schema-store-cassandra.toml /etc/metrictank/schema-store-cassandra.toml
COPY scripts/config/schema-idx-cassandra.toml /etc/metrictank/schema-idx-cassandra.toml
COPY scripts/config/schema-store-scylladb.toml /usr/share/metrictank/examples/schema-store-scylladb.toml
COPY scripts/config/schema-idx-scylladb.toml /usr/share/metrictank/examples/schema-idx-scylladb.toml

COPY --from=gobase /tmp/build/* /usr/bin/

COPY scripts/util/wait_for_endpoint.sh /usr/bin/wait_for_endpoint.sh

EXPOSE 6060

ENTRYPOINT ["/usr/bin/wait_for_endpoint.sh"]
CMD ["/usr/bin/metrictank", "-config=/etc/metrictank/metrictank.ini"]
