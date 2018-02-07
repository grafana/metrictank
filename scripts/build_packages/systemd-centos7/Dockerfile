FROM centos:7

ENV BASE_PATH=/go/src/github.com/grafana/metrictank

ARG PKG
ARG VERSION
ARG ARCH
COPY . $BASE_PATH/
RUN yum install -y git ruby rubygems ruby-devel openssl-devel cyrus-sasl-devel
RUN yum groupinstall -y 'Development Tools'
RUN gem install --no-ri --no-rdoc fpm
RUN $BASE_PATH/scripts/get_go.sh
ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOPATH="/go"
RUN $BASE_PATH/scripts/build.sh
RUN $BASE_PATH/scripts/build_tools.sh
RUN $BASE_PATH/scripts/build_packages/systemd-centos7/build_package.sh
