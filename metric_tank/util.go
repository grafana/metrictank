package main

import (
	"errors"
)

const maxTTL = uint32(65535 * 3600)

var errTTLTooHigh = errors.New("TTL value too high. can be max 2730 days (65535 * 3600 seconds)")

func min32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func min16(a, b uint16) uint16 {
	if a < b {
		return a
	}
	return b
}

func max16(a, b uint16) uint16 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// lcm returns the least common multiple
func lcm(vals []uint16) uint16 {
	out := vals[0]
	for i := 1; i < len(vals); i++ {
		a := max16(vals[i], out)
		b := min16(vals[i], out)
		r := a % b
		if r != 0 {
			for j := uint16(2); j <= b; j++ {
				if (j*a)%b == 0 {
					out = j * a
					break
				}
			}
		} else {
			out = a
		}
	}
	return out
}

func hourlyTTL(secs uint32) (uint16, error) {
	if secs > maxTTL {
		return 0, errTTLTooHigh
	}
	hours := uint16(secs / 3600)
	if secs%3600 != 0 {
		hours += 1
	}
	return hours, nil
}
