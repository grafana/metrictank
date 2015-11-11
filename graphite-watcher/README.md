run this tool against a graphite server

* at 0.1Hz checks which metrics are in ES
* at 10Hz selects random metrics and executes graphite queries on them

it verifies the graphite output on format, ordering, null values etc.
and throws errors if anything about the data looks off,
and also submits some useful metrics into the monitoring system.

(protip: 1 endpoint with default settings has 30 metrics)
useful when running with env-load.
