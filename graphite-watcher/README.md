run this tool against a graphite server

* it gets all known metrics from ES and then verifies the number of known metrics
(protip: 1 endpoint with default settings has 30 metrics)
useful when running with env-load.

* then it runs queries at 10Hz, by selecting random metrics,
  and verifies the graphite output on format, ordering, null values etc.
