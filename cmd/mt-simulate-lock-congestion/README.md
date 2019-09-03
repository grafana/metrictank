
This tool runs an experiment:
it simulates a workload on the memory-idx, by preloading it with data, and then doing a benchmark stresstest in which we simultaneously issue queries, adds and updates, all at configurable rates. and measure the durations of each operation.

## the flow
* pre-experiment
* runs experiment
* waits run-duration, then cancels everything
* print stats
* writes profiles as needed

## pre-experiment

* creates memory index
* pre-populates index with initial-index-size entries from series-file.
  these are all adds to the extent that initial-index-size is smaller than the input file. once we reach the end of the file, we re-use series.


## the experiment

* issue queries at given rate queries-per-sec.
  queries straight from file queries-file.
* issues AddOrUpdate calls from series-file.
  typically updates, but upto new-series-percent is adds (except if we've already seen them), so the real add percentage is is <= new-series-percent.
  partition is hashed from name
  runs add-threads threads to add data to the index, each thread corresponds to 1 partition in the index.
  the threads are fed data by the router, which routes data from the metrics generator, but limits it to rate of adds-per-sec // called adds but could these be updates?

## obtaining the files

TODO

