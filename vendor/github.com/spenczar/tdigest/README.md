# tdigest #
[![GoDoc](https://godoc.org/github.com/spenczar/tdigest?status.svg)](https://godoc.org/github.com/spenczar/tdigest) [![Build Status](https://travis-ci.org/spenczar/tdigest.svg)](https://travis-ci.org/spenczar/tdigest)

This is a Go implementation of Ted Dunning's
[t-digest](https://github.com/tdunning/t-digest), which is a clever
data structure/algorithm for computing approximate quantiles of a
stream of data.

You should use this if you want to efficiently compute extreme rank
statistics of a large stream of data, like the 99.9th percentile.

## Usage ##

An example is available in the Godoc which shows the API:

```go
func ExampleTDigest() {
	rand.Seed(5678)
	values := make(chan float64)

	// Generate 100k uniform random data between 0 and 100
	var (
		n        int     = 100000
		min, max float64 = 0, 100
	)
	go func() {
		for i := 0; i < n; i++ {
			values <- min + rand.Float64()*(max-min)
		}
		close(values)
	}()

	// Pass the values through a TDigest.
	td := New()

	for val := range values {
		// Add the value with weight 1
		td.Add(val, 1)
	}

	// Print the 50th, 90th, 99th, 99.9th, and 99.99th percentiles
	fmt.Printf("50th: %.5f\n", td.Quantile(0.5))
	fmt.Printf("90th: %.5f\n", td.Quantile(0.9))
	fmt.Printf("99th: %.5f\n", td.Quantile(0.99))
	fmt.Printf("99.9th: %.5f\n", td.Quantile(0.999))
	fmt.Printf("99.99th: %.5f\n", td.Quantile(0.9999))
	// Output:
	// 50th: 48.74854
	// 90th: 89.79825
	// 99th: 98.92954
	// 99.9th: 99.90189
	// 99.99th: 99.98740
}
```

## Algorithm ##

For example, in the Real World, the stream of data might be *service
timings*, measuring how long a server takes to respond to clients. You
can feed this stream of data through a t-digest and get out
approximations of any quantile you like: the 50th percentile or 95th
percentile or 99th or 99.99th or 28.31th are all computable.

Exact quantiles would require that you hold all the data in memory,
but the t-digest can hold a small fraction - often just a few
kilobytes to represent many millions of datapoints. Measurements of
the compression ratio show that compression improves super-linearly as
more datapoints are fed into the t-digest.

How good are the approximations? Well, it depends, but they tend to be
quite good, especially out towards extreme percentiles like the 99th
or 99.9th; Ted Dunning found errors of just a few parts per million at
the 99.9th and 0.1th percentiles.

Error will be largest in the middle - the median is the least accurate
point in the t-digest.

The actual precision can be controlled with the `compression`
parameter passed to the constructor function `NewWithCompression` in
this package. Lower `compression` parameters will result in poorer
compression, but will improve performance in estimating quantiles. If
you care deeply about tuning such things, experiment with the
compression ratio.

## Benchmarks ##

Data compresses well, with compression ratios of around 20 for small
datasets (1k datapoints) and 500 for largeish ones (1M
datapoints). The precise compression ratio depends a bit on your
data's distribution - exponential data does well, while ordered data
does poorly:

![compression benchmark](docs/compression_benchmark.png)

In general, adding a datapoint takes about 1 to 4 microseconds on my
2014 Macbook Pro. This is fast enough for many purposes, but if you
have any concern, you should just run the benchmarks on your targeted
syste. You can do that with `go test -bench . ./...`.

Quantiles are very, very quick to calculate, and typically take tens
of nanoseconds. They might take up to a few hundred nanoseconds for
large, poorly compressed (read: ordered) datasets, but in general, you
don't have to worry about the speed of calls to Quantile.
