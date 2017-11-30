## Artisanal histogram


Hand crafted histograms, made with love. To power insights from networked applications.  Not general-purpose.
Also somewhat experimental.


### goals

* optimize for typical range of networked applications, where we care for durations between roughly 1ms and 15s.
  anything under a ms is plenty fast.  Even if it was a a microsecond or less, we don't mind it being reported in the 1ms bucket.
  Likewise, anything over 15s is awful.  Whether it's 15s, 20s or 30s. Doesn't really matter.  They are all terrible and can go in the same bucket.
  Contrast this to [hdrhistograms](https://github.com/codahale/hdrhistogram) which are designed to provide buckets which can provide close approximations over huge ranges which I don't actually care about.
  This way we can also store the data in a more compact fashion.
* understandability of the class intervals ("buckets"), eg have rounded intervals that show well on UI's.
  powers of two are [faster to compute](http://pvk.ca/Blog/2015/06/27/linear-log-bucketing-fast-versatile-simple/) but then your buckets are like 1024, 1280, etc.
  I want to be able to answer questions like "how many requests were completed within 5 milliseconds? how many in a second or less"?
  Every histogram can return percentiles with a given degree of error, often configurable.
  We allow for a bit more error in the typical case (and much more error for extreme outliers such as <<1ms and >>15s) in return for accurate numbers in histograms the way people actually want to look at them.
* consistent bucket sizes across different histograms so we can easily aggregate different histograms together (e.g. for timeseries rollups or runtime consolidation).
(this rules out [gohistogram](https://github.com/VividCortex/gohistogram)
* give equal weight to all samples within a given observation interval, and no weight to samples from prior intervals (contrast to EWMA based approaches)
* good enough performance to not be an overhead for applications doing millions of histogram adds per second.  See below

### performance

Performance is not riduculously fast like [some of the histograms](https://github.com/dgryski/go-linlog) that only need a few instructions per Add because their buckets have boundaries optimized for powers of two.  We have "human friendly" buckets, so our adds are up to about 20ns (e.g. 1M/second at 5% cpu usage).  which is fast enough for now, but could be improved more later.
Getting a report takes about 700ns.

On my i7-4810MQ CPU @ 2.80GHz : 

```
Benchmark_AddDurationBest-8               	100000000	        12.7 ns/op
Benchmark_AddDurationWorst-8              	100000000	        12.6 ns/op
Benchmark_AddDurationEvenDistribution-8   	100000000	        19.5 ns/op
Benchmark_AddDurationUpto1s-8             	100000000	        20.8 ns/op
Benchmark_Report1kvals-8                  	 3000000	       566 ns/op
PASS
ok  	github.com/Dieterbe/artisanalhistogram/hist1	117.842s
```

### warning

if it wasn't clear yet, you need to understand the implications of this approach. For data <1ms or >15s the data will be significantly different from actual results, however, the conclusions
will be the same (in the first case "everything is great" and in the latter "our system is doing terribly"). statistical summaries such as means which are already misleading on their own can get even more misleading, so pay attention to what the histogram buckets say. Those are the source of thruth.


### buckets

the following classes (buckets) have been manually crafted in an effort to
* cover the space well
* represent boundaries people actually care about
* still minimize errors as well as possible, by scaling up the class intervals corresponding to the bucket boundaries.

boundaries are in ms.
exactly 32 buckets. (32x4=128B size)

```
1
2
3
5
7.5
10
15
20
30
40
50
65
80
100
150
200
300
400
500
650
800
1000
1500
2000
3000
4000
5000
6500
8000
10000
15000
inf
```

### implementation notes

* 32 buckets because that should fit nicely on graphical UI's + also round size of 128B
* math.MaxUint32, i.e. 4294967295 or "4 billion" should be a reasonable count limit. to save space.
  it's up to the operator to keep tabs on the bucket counts and judge whether the data is to be trusted or not.
  if you get anywhere near this volume it's time to look into something better or shorten your reporting interval.
  unfortunately trying to automatically report almost overflows would be to expensive.
  Note that for the total count (across all buckets) we do report validity, because that's easy to establish when reporting.
  but validity of individual buckets is up to the user.
* because we use uint32 to track microseconds, inserting durations higher than 4294s (71minutes) will also overflow and possibly fall into the wrong buckets
