FROM grafana/metrictank
RUN apk add --no-cache curl jq ca-certificates python py-pip
RUN pip install kazoo
COPY entrypoint.sh /entrypoint.sh
COPY getOffset.py /getOffset.py
ENTRYPOINT ["/entrypoint.sh"]
