FROM alpine
MAINTAINER Anthony Woods awoods@raintank.io

RUN mkdir -p /etc/raintank
COPY config/metrictank-docker.ini /etc/raintank/metrictank.ini
COPY config/storage-schemas.conf /etc/raintank/storage-schemas.conf

COPY build/metrictank /usr/bin/metrictank
COPY wait_for_endpoint.sh /usr/bin/wait_for_endpoint.sh

EXPOSE 6060

ENTRYPOINT ["/usr/bin/wait_for_endpoint.sh"]
CMD ["/usr/bin/metrictank", "-config=/etc/raintank/metrictank.ini"]
