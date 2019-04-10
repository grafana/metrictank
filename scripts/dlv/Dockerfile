FROM golang:1.11.4-alpine AS gobase
ENV CGO_ENABLED 0

# Compile Delve
RUN apk add --no-cache git
RUN go get github.com/derekparker/delve/cmd/dlv

FROM alpine
MAINTAINER Dieter Plaetinck dieter@grafana.com

RUN apk add -U tzdata

RUN mkdir -p /etc/metrictank /usr/share/metrictank/examples
COPY config/metrictank-docker.ini /etc/metrictank/metrictank.ini
COPY config/index-rules.conf /etc/metrictank/index-rules.conf
COPY config/storage-schemas.conf /etc/metrictank/storage-schemas.conf
COPY config/storage-aggregation.conf /etc/metrictank/storage-aggregation.conf
COPY config/schema-store-cassandra.toml /etc/metrictank/schema-store-cassandra.toml
COPY config/schema-idx-cassandra.toml /etc/metrictank/schema-idx-cassandra.toml
COPY config/schema-store-scylladb.toml /usr/share/metrictank/examples/schema-store-scylladb.toml
COPY config/schema-idx-scylladb.toml /usr/share/metrictank/examples/schema-idx-scylladb.toml

COPY build/* /usr/bin/

COPY util/wait_for_endpoint_debug.sh /usr/bin/wait_for_endpoint_debug.sh

COPY --from=gobase /go/bin/dlv /usr/bin/dlv

EXPOSE 6060
EXPOSE 40000

ENTRYPOINT ["/usr/bin/wait_for_endpoint_debug.sh"]
CMD [ "/usr/bin/dlv", "--listen=:40000", "--headless=true", "--api-version=2", "--log", "--log-output=rpc", "exec", "/usr/bin/metrictank", "--" ,"-config=/etc/metrictank/metrictank.ini" ]
