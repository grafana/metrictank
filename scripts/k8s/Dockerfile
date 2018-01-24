FROM grafana/metrictank
RUN apk add --no-cache curl jq ca-certificates
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
