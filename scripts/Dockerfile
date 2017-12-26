FROM alpine
MAINTAINER Dieter Plaetinck dieter@grafana.com

RUN apk add -U tzdata

RUN mkdir -p /etc/metrictank
COPY config/metrictank-docker.ini /etc/metrictank/metrictank.ini
COPY config/storage-schemas.conf /etc/metrictank/storage-schemas.conf
COPY config/storage-aggregation.conf /etc/metrictank/storage-aggregation.conf

COPY build/* /usr/bin/

COPY util/wait_for_endpoint.sh /usr/bin/wait_for_endpoint.sh

EXPOSE 6060

ENTRYPOINT ["/usr/bin/wait_for_endpoint.sh"]
CMD ["/usr/bin/metrictank", "-config=/etc/metrictank/metrictank.ini"]
