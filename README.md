[![Circle CI](https://circleci.com/gh/raintank/raintank-metric.svg?style=shield)](https://circleci.com/gh/raintank/raintank-metric)

this repo contains some common libraries, schema and message definitions, as well as the following apps who all pull data from NSQ:

* nmt : nsq_metrics_tank (`tank` branch)
* nme : nsq_metrics_to_elasticsearch (maintains metric definitions in ES)
* nmk : nsq_metrics_to_kairos (stores metrics data in kairosdb)
* nms : nsq_metrics_to_stdout: handy for manually looking which metrics are going through NSQ
* npee: nsq_probe_events_to_elasticsearch (stores prove events to ES)

# format used

aims to be generic, and at the very least not litmus-specific.
metrics are defined in schema.MetricData, but multiple are packed in 1 NSQ message, see msg.MetricData
probe events are in 1 message per event, and is defined in schema.ProbeEvent


License
=======

This software is copyright 2015 by Raintank, Inc. and is licensed under the
terms of the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

or in the root directory of this package.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
