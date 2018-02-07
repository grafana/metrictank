ARG BASE_PATH=/go/src/github.com/grafana/metrictank

# build binaries inside an alpine3.7 container
FROM golang:1.9.3-alpine3.7 AS build
ARG BASE_PATH

RUN apk --update add build-base linux-headers openssl-dev cyrus-sasl-dev git bash python

COPY . $BASE_PATH/
RUN $BASE_PATH/scripts/build.sh


# now build the production image based on the same alpine version
# as was used to build the binaries in
FROM alpine:3.7
ARG BASE_PATH

LABEL maintainer="Dieter Plaetinck dieter@grafana.com"

RUN apk add -U ca-certificates openssl cyrus-sasl tzdata

RUN mkdir -p /etc/metrictank
COPY scripts/config/metrictank-docker.ini /etc/metrictank/metrictank.ini
COPY scripts/config/storage-schemas.conf /etc/metrictank/storage-schemas.conf
COPY scripts/config/storage-aggregation.conf /etc/metrictank/storage-aggregation.conf

# copy the built binaries from the build image
COPY --from=build $BASE_PATH/build/* /usr/bin/

COPY scripts/util/wait_for_endpoint.sh /usr/bin/wait_for_endpoint.sh

EXPOSE 6060

ENTRYPOINT ["/usr/bin/wait_for_endpoint.sh"]
CMD ["/usr/bin/metrictank", "-config=/etc/metrictank/metrictank.ini"]

