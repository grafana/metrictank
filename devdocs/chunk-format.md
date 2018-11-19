# Chunk format

## chunk body

We have 3 different chunk formats (see mdata/chunk package for implementation)

| Name                         | Contents                         |
| ---------------------------- | -------------------------------- |
| FormatStandardGoTsz          | `<format><tsz.Series4h>`         |
| FormatStandardGoTszWithSpan  | `<format><span><tsz.Series4h>`   |
| FormatGoTszLongWithSpan      | `<format><span><tsz.SeriesLong>` |

* format is encoded as a 1-byte unsigned integer.
* span encodes chunkspans upto 24h via a 1-byte shorthand code.
* the tsz.Series data is timeseries data encoded via the Facebook Gorilla compression mechanism. See below

## tsz timeseries data

There are some subtle differences between the paper and our implementation/use
The paper lives at http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

It describes:

`t-1` is "initializer timestamp", aligned to a 2h window, though there doesn't seem to a real need for the alignment
`t0` is ts of first data point

Our implementation is based on [go-tsz](https://github.com/dgryski/go-tsz) where `t0` is passed by the caller and is the initializer/header timestamp.
it encodes it uncompressed without caring whether it is aligned to anything. It doesn't have a concept of a `t-1`.

In MT, at chunk creation time, we pass in the t0 to go-tsz which is aligned to chunkspan.  From then on, we add values normally.

Now let's talk about our different formats

### tsz.Series4h

Based on upstream go-tsz. With one important exceptional patch. Read below.
The stream looks like so:

```
<t0 uint32><14b delta><float64><dod><xordelta>[...]<end-of-stream-markerV1>
```

First t0 is stored as uint32, then the first (t,v) as 14bit delta and 64bit value, then for all subsequents points the compression as described in the paper kicks in. with full 32bit delta-of-delta encoding if the range is needed.
We've made the mistake of using this format for chunkspans of more than 4hours, which it doesn't support, as for very sparse data, the initial 14-bit delta can overflow.
Luckily we can mostly recover the broken data based on the knowledge that delta's can never be negative, and using hints that timestamps should align to a certain interval.


### tsz.SeriesLong

Newer version of tsz.Series4h with these differences:
* no t0, as we always track the t0 alongside the data anyway (both in storage and in memory)
* no initial delta: use delta-of-delta compression directly, assuming a starting delta of 60 against t0.
* more compact end of stream marker (see below)

The stream looks like so:
```
<dod><float64><dod><xordelta>[...]<end-of-stream-markerV2>
```

### end-of-stream marker

This marker helps the decoder to realize there is no more data (as opposed to the start of a point).
We use two versions of markers: V1 and V2

In tsz.SeriesLong, a point always starts with a dod value. (In tsz.Series4h also, assuming a chunk always contains at least one point, which is true in how we use it)
Thus, we only need to be able to differentiate between a dod and the end-of-stream marker.

go-tsz, and hence tsz.Series4h use an end-of-stream marker of 36 1 bits followed by a zero bit. (The author of the library does not remember why the zero bit is there)
Yet, in go-tsz and tsz.Series4h dod encoded values are any of these forms:

```
0                   // dod 0
10<7bit value>      // dod in range [-63,64]
110<9bit value>     // dod in range [-255,256]
1110<12bit value>   // dod in range [-2047,2048]
1111<32bit value>   // any other dod value
```

The end-of-stream marker is then a special case of the last variant, with the 32 bit value being all 1's. (`int32(-1)` which would be an impossible dod value for that case, thus the marker can be detected)

When working on tsz.SeriesLong, we realized we don't need that many bits to represent the end-of-stream marker and instead came up with this:

```
0                   // dod 0
10<7bit value>      // dod in range [-63,64]
110<9bit value>     // dod in range [-255,256]
1110<12bit value>   // dod in range [-2047,2048]
11110<32bit value>   // any other dod value
```

The last case has received an extra '0' bit, which means we can simply use `11111` as end-of-stream marker, saving several bytes.
The last dod value is rare, so the overhead is negligible.

## shortcomings of our chunk formats

This is a brainstorm of some ideas on how we might be able to improve our formats in the future.

* metrictank uses fixed intervals, do we even need to store timestamps. could maybe store number of nulls since last point.
  esp. long chunks also have long fixed delta's. perhaps we can use different chunk format for raw (unquantized data) vs aggregated.
  or instead of storing raw unquantized data and quantizing at readtime, we should probably quantize before storing.
* go-tsz: last dod case (outside [-2047,2048]) means we use 32bit to store dod. for sparse data, perhaps we can come up with something better


