an opinionated wrapper around metric client libraries

imported from Grafana ffcc807ed34f853a8bc9600bcf7801547a5feb4f

supports:
* statsd (recommended!)
* dogstatsd

and later maybe more, like go-metrics


Why?
* make it easy to switch between libraries.
* some libraries just take in string arguments for gauge names etc, but it's nicer to have variables per object for robustness, especially when used in multiple places, and gives a central overview.
* allows you to set deleteGauges and deleteStats in your statsd servers ( a good thing for stateless servers), cause we will automatically keep a gauge sending.
