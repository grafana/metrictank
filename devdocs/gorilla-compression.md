There are some subtle differences between the paper and our implementation/use

### paper
http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

`t-1` is "initializer timestamp", aligned to a 2h window, though there doesn't seem to a real need for the alignment
`t0` is ts of first data point

### our implementation

in [go-tsz](https://github.com/dgryski/go-tsz) `t0` is passed by the caller and is the initializer/header timestamp.
it encodes it uncompressed without caring whether it is aligned to anything


In MT, at chunk creation time, we pass in the t0 to go-tsz which is aligned to chunkspan.
from then on, add values normally.

So storage wise: first t0 is stored as uint32, then (t,v) as 14bit delta and 64bit value, then for all subsequents points  the compression kicks in. with full 32bit delta of delta encoding if the range is needed.

At read time, creating iterator reads first 32bit, then first value (reads 14 and 64bit), then the rest with true decompression

```
<t0 uint32><14b delta><float64><dod><xordelta>...
```

### shortcomings?

* for us: fixed interval, do we even need to store timestamps. could maybe store number of nulls since last point.
  esp. long chunks also have long fixed delta's. perhaps we can use different chunk format for raw (unquantized data) vs aggregated.
  or instead of storing raw unquantized data and quantizing at readtime, we should probably quantize before storing.
* go-tsz: large dod (>2048) means we use 32bit to store dod(!). not good for sparse data
* more compact end of stream marker, e.g. just 5 1's:
  the only concern would be that it would be ambiguous with the code of 4 1's followed by a a value that starts with a 1.
  but that case would be a uint32 starting with a 1, which would mean a dod value of more than 2 billion. so that can't really happen.
  this would save 32bits, or 4 bytes. if the large dod (>2048) is very common, then it might be a slowdown to always check for the one extra bit.
