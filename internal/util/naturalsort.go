package util

import (
	"strconv"
)

// NaturalSortStringSlice sorts strings lexicographically *except* substrings of numbers are sorted numerically
type NaturalSortStringSlice []string

func (ss NaturalSortStringSlice) Len() int {
	return len(ss)
}

func (ss NaturalSortStringSlice) Less(i, j int) bool {
	return NaturalLess(ss[i], ss[j])
}

func (ss NaturalSortStringSlice) Swap(i, j int) {
	ss[i], ss[j] = ss[j], ss[i]
}

// NaturalLess compares strings lexicographically *except* substrings of numbers are sorted numerically
func NaturalLess(s, t string) bool {
	sItr := chunkIter{base: s, pos: 0}
	tItr := chunkIter{base: t, pos: 0}

	for sItr.hasNext() && tItr.hasNext() {
		sChunk := sItr.nextChunk()
		tChunk := tItr.nextChunk()

		res := compareChunks(sChunk, tChunk)

		if 0 == res {
			continue
		}

		return res < 0
	}

	return tItr.hasNext() && !sItr.hasNext()
}

// Used to iterate over strings in "chunks" of either entirely digits or entirely not digits
type chunkIter struct {
	base string
	pos  int
}

func (c *chunkIter) hasNext() bool {
	return c.pos < len(c.base)
}

func (c *chunkIter) nextChunk() string {
	isNumeric := IsDigit(c.base[c.pos])
	endPos := c.pos + 1
	for endPos < len(c.base) && IsDigit(c.base[endPos]) == isNumeric {
		endPos++
	}
	ret := c.base[c.pos:endPos]
	c.pos = endPos
	return ret
}

func compareChunks(a, b string) int {
	if a == b {
		return 0
	}

	if IsDigit(a[0]) && IsDigit(b[0]) {
		// Both numeric, compare as numbers
		aNum, _ := strconv.Atoi(a)
		bNum, _ := strconv.Atoi(b)
		return aNum - bNum
	}

	// Not both numeric, compare as strings
	if a < b {
		return -1
	}
	return 1
}
