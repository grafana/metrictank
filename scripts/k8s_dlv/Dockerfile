FROM grafana/metrictank-debug
RUN apk add --no-cache curl jq ca-certificates python py-pip
RUN pip install kazoo
COPY entrypoint.sh /entrypoint.sh
COPY getOffset.py /getOffset.py
ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/usr/bin/dlv", "--listen=:40000", "--headless=true", "--api-version=2", "--log", "--log-output=rpc", "exec", "/usr/bin/metrictank", "--" ,"-config=/etc/metrictank/metrictank.ini" ]
