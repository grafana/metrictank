# Compression tips

* values that never - or infrequently - change compress extremely well, so are very cheap to track and store.
* pay attention to your timestamps, make sure they are evenly spaced. That compresses better.
* storing values as integers (or more precisely: floats without decimal numbers) compresses very well.
  So you best store the numbers with the same unit as what your precision is.
  E.g. let's say you measure latencies such as 0.035 seconds (3 decimals precision, e.g. ms precision), it's better to
  track that as the number 35 (milliseconds) instead of 0.035 (seconds).

For more details, see the [go-tsz eval program](https://github.com/dgryski/go-tsz/tree/master/eval) or the 
[results table](https://raw.githubusercontent.com/dgryski/go-tsz/master/eval/eval-results.png)
