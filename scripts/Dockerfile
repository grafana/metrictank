FROM alpine
MAINTAINER Anthony Woods awoods@raintank.io

RUN mkdir -p /etc/raintank
COPY config/metrictank.ini /etc/raintank/metrictank.ini
COPY config/storage-schemas.conf /etc/raintank/storage-schemas.conf

COPY build/metrictank /usr/bin/metrictank
COPY metrictank_entrypoint.sh /usr/bin/

EXPOSE 6060

ENTRYPOINT ["/usr/bin/metrictank_entrypoint.sh"]
CMD ["-config=/etc/raintank/metrictank.ini"]