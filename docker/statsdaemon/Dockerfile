FROM golang:1.6

ENV GOPATH /go
ENV PATH $PATH:/usr/local/go/bin:$GOPATH/bin
RUN go get github.com/raintank/statsdaemon/statsdaemon
COPY statsdaemon.ini /etc/statsdaemon.ini
CMD ["statsdaemon"]
