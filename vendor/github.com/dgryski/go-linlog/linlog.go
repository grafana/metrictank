// Package linlog implements linear-log bucketing
/*

http://pvk.ca/Blog/2015/06/27/linear-log-bucketing-fast-versatile-simple/

*/
package linlog

import (
	"math/bits"
)

// BinOf rounds size as appropriate, and returns the rounded size and bucket number.
func BinOf(size uint64, linear, subbin uint64) (rounded uint64, bucket uint64) {
	nBits := lb(size | (1 << uint(linear)))
	shift := nBits - subbin
	mask := uint64(1<<shift - 1)
	round := size + mask /* XXX: overflow. */
	subIndex := round >> shift
	xrange := nBits - linear

	return (round & ^mask), (xrange << subbin) + subIndex
}

// BinDownOf rounds size down, and returns the rounded size and bucket number.
func BinDownOf(size uint64, linear, subbin uint64) (rounded uint64, bucket uint64) {
	nBits := lb(size | (1 << linear))
	shift := nBits - subbin
	subIndex := size >> shift
	xrange := nBits - linear

	return (subIndex << shift), (xrange << subbin) + subIndex
}

func Bins(max uint64, linear, subbin uint64) []uint64 {
	var buckets []uint64
	buckets = append(buckets, 0)

	var size uint64
	var incr uint64 = 1 << (linear - subbin)

	for size < (1 << linear) {
		size += incr
		if size >= max {
			break
		}
		buckets = append(buckets, size)
	}

	for size < max {
		for steps := uint64(0); steps < (1 << subbin); steps++ {
			size += incr
			buckets = append(buckets, size)
			if size > max {
				break
			}
		}
		incr <<= 1
	}

	return buckets
}

func lb(x uint64) uint64 {
	return uint64(63 - bits.LeadingZeros64(x))
}
