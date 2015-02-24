/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
This is "raintank-metrics", a golang implementation of the nodejs
raintank-worker program. The documentation is a work in progress, but in the
meantime here are the command-line flags while the documention gets written:

	Usage:
	  raintank-metric [OPTIONS]

	Application Options:
	  -V, --verbose               Show verbose debug information. Repeat for more
				      verbosity.
	  -c, --config=               Specify a configuration file.
	  -L, --log-file=             Log to file X
	  -s, --syslog                Log to syslog rather than a log file.
				      Incompatible with -L/--log-file.
	  -g, --graphite-addr=        Graphite IP address or hostname.
	  -p, --graphite-port=        Port graphite listens on.
	  -d, --elasticsearch-domain= Elasticseach domain.
	  -t, --elasticsearch-port=   Port to connect to for Elasticsearch, defaults to
				      9200.
	  -u, --elasticsearch-user=   Optional username to use when connecting to
				      elasticsearch.
	  -P, --elasticsearch-passwd= Optional password to use when connecting to
				      elasticsearch.
	  -r, --redis-addr=           Hostname or IP address of redis server.
	  -y, --redis-passwd=         Optional password to use when connecting to redis.
	  -D, --redis-db=             Option database number to use when connecting to
				      redis.

	Help Options:
	  -h, --help                  Show this help message

License

This software is copyright 2015 by Raintank, Inc. and is licensed under the
terms of the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

or in the root directory of this package.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/
package main
