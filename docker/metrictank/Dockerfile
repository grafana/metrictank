FROM golang:1.6

ENV GOPATH /go
ENV PATH $PATH:/usr/local/go/bin:$GOPATH/bin
RUN go get github.com/raintank/metrictank
RUN apt-get update && apt-get install -y netcat

RUN mkdir /etc/raintank
ADD wait.sh /usr/bin/wait.sh
ADD storage-schemas.conf /etc/raintank/storage-schemas.conf
ADD metrictank.ini /etc/raintank/metrictank.ini

CMD wait.sh cassandra:9042 && /go/bin/metrictank
