FROM ubuntu:14.04

ENV BASE_PATH=/go/src/github.com/grafana/metrictank

ARG PKG
ARG VERSION
ARG ARCH
COPY . $BASE_PATH/
RUN apt-get update && apt-get install -y curl build-essential libssl-dev libsasl2-dev git bash python pkg-config ruby ruby-dev
RUN gem install --no-ri --no-rdoc fpm
RUN $BASE_PATH/scripts/get_go.sh
ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOPATH="/go"
RUN $BASE_PATH/scripts/build.sh
RUN $BASE_PATH/scripts/build_tools.sh
RUN $BASE_PATH/scripts/build_packages/sysvinit/build_package.sh
