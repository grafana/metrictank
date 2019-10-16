FROM golang:1.13.1-alpine AS gobase
RUN apk add --no-cache git bash

# Compile metrictank
ENV MT_SRC_DIR $GOPATH/src/github.com/grafana/metrictank
RUN mkdir -p $MT_SRC_DIR
ADD . $MT_SRC_DIR
RUN $MT_SRC_DIR/scripts/build.sh -debug
RUN cp -r $MT_SRC_DIR/build /tmp/build

# Compile Delve
ENV CGO_ENABLED 0
RUN go get github.com/derekparker/delve/cmd/dlv


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

COPY scripts/util/wait_for_endpoint_debug.sh /usr/bin/wait_for_endpoint_debug.sh

COPY --from=gobase /go/bin/dlv /usr/bin/dlv

EXPOSE 6060
EXPOSE 40000

ENTRYPOINT ["/usr/bin/wait_for_endpoint_debug.sh"]
CMD [ "/usr/bin/dlv", "--listen=:40000", "--headless=true", "--api-version=2", "--log", "--log-output=rpc", "exec", "/usr/bin/metrictank", "--" ,"-config=/etc/metrictank/metrictank.ini" ]
