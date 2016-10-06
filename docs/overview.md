# Overview

![Overview](https://raw.githubusercontent.com/raintank/metrictank/master/docs/assets/metrictank-highlevel.png)

Note:
* You can use any, or multiple of the input mechanisms
* multi-tenant setups need [tsdb-gw](https://github.com/raintank/tsdb-gw) in between Grafana and Graphite to authenticate requests before proxying requests to Graphite/Metrictank
* tsdb-gw can also be used as a public ingestion point with native metrics2.0 input, for multi-tenant setups.  Clients can then use [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng<Paste>) to send data.
