# Schema conversion logic for Whisper data

Metrictank comes with schema conversion logic (`mdata/importer/converter.go`) which can be used to convert data from one storage schema into another one. This is currently only used by the Whisper importer, but may in the future also be used to import from other types of data sources.

The conversion happens on a destination archive by destination archive basis. In this context the term "archive" refers to one section between the `,` of a storage schema, f.e. the schema `1s:1d,10min:1y` has two archives. Each destination archive gets generated separately, in some cases the same input archives may be used to generate multiple destination archives and in others different input archives may be used. Multiple input archives may also be combined into a single destination archive, if this makes sense given the input schema and the destination schema.

This is how the schema conversion logic generates a destination archive:

  * In the set of source archives which it has received as input, it finds the archive with the shortest retention which still contains enough data to satisfy the retention of the destination archive
  * In the same set of source archives, it finds the archive with the longest retention that still has a higher or equal resolution as the destination archive
  * At this point it knows that for the generation of the destination archive's points, it will have to use the range of input archives determined at the previous two steps as input, including the ones between them. All other input archives can be ignored. That's because it is trying to satisfy the retention period of the destination archive while also trying to achieve the highest possible resolution up to the resolution of the destination archive at the same time. For example if the destination archive is `10s:180d`, the input archives are `10s:30d,10min:180d`, then it uses both of the input archives to achieve `10s` resolution for the first `30d` and then `10min` resolution for the remaining `150d` in the output. If there was another input archive with the schema `1h:5y`, then it could be ignore for the purpose of generating a `10s:180d` archive.
  * It pre-allocates the set of points to fill the destination archive
  * It iterates over all the input archives that have been chosen in the previous steps, sorted by resolution in decreasing order (starting with the largest interval). It uses all the points within the time range of the destination archive's retention period as input to fill in the points of the destination archive. If the interval of an input archive is higher than the interval of the destination archive, then it "fake-deaggregates" them based on the aggregation method which the input archive was aggregated with. If the interval of an input archive is lower than the interval of the destination archive, then it aggregates the points using the aggregation method which the input archive was aggregated with.
  * Additionally, if the aggregation method used is "average" and the destination archive is not the first/raw archive of the destination schema, then it generates two output archives where one is aggregated by "count" and the other by "sum", because that is what Metrictank expects
  * If the aggregation method of the input archive is "sum" then the conversion generates an archive with aggregation method "sum", plus also one with the aggregation method "cnt", so that the metric would also be queriable by average if the user wants to do so

Some examples to illustrate what the above procedure produces when it generates the destination archives:

#### Increasing the interval

In the above described process, it can happen that some of the used input archives have a lower interval than the destination archive. In this case the conversion process will use the same aggregation logic as Metrictank uses internally to generate rollups to decrease the resolution (increase interval) of the input archive(s) to make it match the destination archive.

#### Decreasing the interval

In the conversion process it can also happen that some of the used input archives have a higher interval than the destination archive. In this case the conversion logic "undoes" the aggregation as good as possible, obviously it's not possible to do this perfectly because during the aggregation some information gets lost. It works differently depending on the aggregation method.

##### Sum

It takes each value and divides it by the ratio of `input interval/output interval`, then it repeats that value `ratio` times. Additionally it creates the archive for the `count` aggregation method, so that the resulting data could also be viewed as `average` by Metrictank if wanted because when requesting an `average` rollup from Metrictank it uses the `sum` and the `count` rollup. 

For example when converting `2` datapoints from a `5s` interval archive which has been aggregated with `sum` into a `1s` interval archive, then the point values would look like this:

```
Input:
  Sum: [40 65]
Output:
  Sum: [8 8 8 8 8 13 13 13 13 13]
  Cnt: [1 1 1 1 1 1 1 1 1 1]
```

Because the ratio between `5s` interval and `1s` interval is `5`, so `40/5=8` and `65/5=13`.

##### Average

When decreasing the interval of an archive with the aggregation method `average`, the behavior is different depending on whether the destination archive is the first/raw archive or a rollup, because Metrictank treats them differently. When reading the raw archive of a metric with the aggregation method `average` it directly accesses the raw archive's data which has not been aggregated yet, however when it reads rolled up data then it uses two archives as input where one is aggregated by `sum` and the other by `count`.

When the schema conversion logic needs to decrease the interval (increase resolution) of an archive with the aggregation method `average` and the destination archive is the raw/first archive, then it simply repeats the present datapoints to fill in the gaps. 

For example when converting `2` datapoints from a `5s` interval archive into a `1s` interval archive, then it would look like this:

```
Input:
  Avg: [1 2]
Output:
  Avg: [1 1 1 1 1 2 2 2 2 2]
```

However, if the destination archive for the same input is a rollup, so not the first archive of the schema, then it needs to generate a `sum` and a `count` aggregation because this is what Metrictank expects. Additionally it tries to ensure that the `sums` and `counts` are correct, as if they would have been generated from the first/raw archive. To do that it multiplies all the input points by the ratio of `input interval/output interval` and then duplicates the result `ratio` times, it also sets all the counts to `ratio`. This way the resulting averages stay correct, plus the counts and sums have values which are closer to what they would really be if the raw metric used as input had a value at every point without any gaps.

```
Input:
  Avg: [1 2]
Output:
  Sum: [5 5 5 5 5 10 10 10 10 10]
  Cnt: [5 5 5 5 5 5 5 5 5 5]
```
When viewed in Metrictank it divides the sums by the counts, so the averages stay correct.

##### Last, Min, Max

The cases of the `last`, `min` and `max` aggregation methods are more simple. When decreasing the interval, the point values simply get repeated to fill all the gaps. So for example when converting the following input datapoints which have been aggregated with `last` from a `1s` interval to a `2s` interval, it would look like this:

```
Input:
  Lst: [1 2]
Output:
  Lst: [1 1 2 2]
```

#### Combining multiple input archives into one destination archive

In some cases it is possible that multiple input archives need to be combined into one destination archive. This is because on one hand we are trying to achieve the highest resolution possible up to the resolution of the destination archive, while at the same time we are also trying to fill the whole retention period of the destination archive. This example shows what would happen if two input archives have to be combined into a single destination archive, to achieve both of the beforementioned goals.

```
Aggregation method: sum
Destination archive: 1s:30s
Input archives: 1s:5s,5s:30s
Input Point Values:
  Archive 1:
    Sum: [1 2 3 4 5]
  Archive 2:
    Sum: [15 40 65 90 115 140]
Output Point Values:
  Archive 1:
    Sum: [1 2 3 4 5 8 8 8 8 8 13 13 13 13 13 18 18 18 18 18 23 23 23 23 23 28 28 28 28 28]
    Cnt: [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1]
```
Note that the ratio between input interval and destination interval is `5` so `40/5=8`, `65/5=13`, `90/5=18`, `115/5=23`, `140/5=28`. At the same time, for the first `5` points it used the higher resolution archive to achieve max accuracy.