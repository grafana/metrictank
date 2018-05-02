FROM alpine
MAINTAINER Dieter Plaetinck dieter@grafana.com

RUN apk add -U tzdata

RUN mkdir -p /etc/metrictank /usr/share/metrictank/examples
COPY config/metrictank-docker.ini /etc/metrictank/metrictank.ini
COPY config/storage-schemas.conf /etc/metrictank/storage-schemas.conf
COPY config/storage-aggregation.conf /etc/metrictank/storage-aggregation.conf
COPY config/schema-store-cassandra.toml /etc/metrictank/schema-store-cassandra.toml
COPY config/schema-idx-cassandra.toml /etc/metrictank/schema-idx-cassandra.toml
COPY config/schema-store-scylladb.toml /usr/share/metrictank/examples/schema-store-scylladb.toml
COPY config/schema-idx-scylladb.toml /usr/share/metrictank/examples/schema-idx-scylladb.toml

COPY build/* /usr/bin/

COPY util/wait_for_endpoint.sh /usr/bin/wait_for_endpoint.sh

EXPOSE 6060

ENTRYPOINT ["/usr/bin/wait_for_endpoint.sh"]
CMD ["/usr/bin/metrictank", "-config=/etc/metrictank/metrictank.ini"]
