[![Circle CI](https://circleci.com/gh/raintank/raintank-metric.svg?style=shield)](https://circleci.com/gh/raintank/raintank-metric)

this repo contains:
* schema and message definitions for metrics and events, and some common libraries.
* programs:
  - [metric-tank](https://github.com/raintank/raintank-metric/tree/master/metric_tank): metric storage chunking, compressing, aggregating graphite backend, backed by cassandra
  - nsq_metrics_to_stdout: to see what flows through NSQ
  - nsq_probe_events_to_elasticsearch: consume probe events and store them in elasticsearch.
  - inspect-es: tool to query metric metadata for sanity checks and stress testing
  - fake_metrics_to_nsq: generate a metrics workload to flow into NSQ for stress testing.
  - graphite-watcher: runs sanity checks on graphite server (metric-tank)


# format used

metrics and events are in the [metrics 2.0](http://metrics20.org/) format and messagepack encoded.
see the [schema library](https://github.com/raintank/raintank-metric/tree/master/schema) for the exact format.

metrics are defined in schema.MetricData, but multiple are packed in 1 NSQ message, see msg.MetricData
see [msg](https://github.com/raintank/raintank-metric/tree/master/msg) library.
probe events are in 1 message per event, and is defined in schema.ProbeEvent


License
=======

This software is copyright 2015 by Raintank, Inc. and is licensed under the
terms of the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

or in the root directory of this package.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
