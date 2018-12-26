FROM grafana/metrictank:latest-debug
RUN apk add --no-cache curl jq ca-certificates python py-pip
RUN pip install kazoo
COPY entrypoint_debug.sh /entrypoint_debug.sh
COPY getOffset.py /getOffset.py
ENTRYPOINT ["/entrypoint_debug.sh"]
CMD [ "/usr/bin/dlv", "--listen=:40000", "--headless=true", "--api-version=2", "--log", "--log-output=rpc", "exec", "/usr/bin/metrictank", "--" ,"-config=/etc/metrictank/metrictank.ini" ]
