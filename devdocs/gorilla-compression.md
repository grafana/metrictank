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
+
+
+
+
