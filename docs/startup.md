# Grafana Metrictank startup

The full startup procedure has many details, but here we cover the main steps if they affect:

* performance/resource usage characteristics
* cluster status
* API availability
* diagnostics


| Phase                   | Description                                                                                        | effect on CPU / RAM                 |
| ----------------------- | -------------------------------------------------------------------------------------------------- | ----------------------------------- |
| load config             | load/validate config                                                                               | no                                  |
| setup diagnostics       | set up logging, profiling, proftrigger                                                             | no                                  |
| log startup             | logs "Metrictank starting" message                                                                 | no                                  |
| start sending stats     | starts connecting and writing to graphite endpoint                                                 | no                                  |
| create Store            | create keyspace, tables, write queues, etc                                                         | minor RAM increase ~ queue size     |
| create Input(s)         | open connections (kafka) or listening sockets (carbon)                                 | no                                  |
| start cluster           | starts gossip, joins cluster                                                                       | no                                  |
| create Index            | creates instance and starts write queues                                                           | minor RAM increase ~ queue size     |
| start API server        | opens listening socket and starts handling requests in not-ready mode                              | no                                  |
| init Index              | creates session, keyspace, tables, write queues, etc and loads in-memory index from persisted data | reasonable RAM and CPU increase                    |
| create cluster notifier | optional: connects to Kafka, starts backfilling persistence message and waits until done or timeout| if backfilling: above-normal CPU, normal RAM usage |
| start input plugin(s)   | starts backfill (kafka) or listening (carbon) and maintain priority based on input lag | if backfilling: above-normal CPU and RAM usage     |
| mark ready state        | immediately (primary) / after warmup (secondary) [details](clustering.md#priority-and-ready-state) | no                                                 |

We recommend provisioning a cluster such that it can backfill a 7 hour backlog in half on hour or less. This means:
* The CPU increase during the kafka backfilling is very significant: typically a 14x cpu increase compared to normal usage.
* The RAM usage during the input data backfilling is typically about 1.5x to 2x normal,
  though the `cluster.gc-percent-not-ready` setting lets you trade cpu for memory usage during startup.

Backfilling will go as fast as it can until it reaches a bottleneck (kafka brokers, cpu constraints, etc), so your numbers may vary.

This is true for v0.11.0, but may need revising later.
