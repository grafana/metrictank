package util

func Min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Lcm returns the least common multiple
func Lcm(vals []uint32) uint32 {
	out := vals[0]
	for i := 1; i < len(vals); i++ {
		max := Max(uint32(vals[i]), out)
		min := Min(uint32(vals[i]), out)
		r := max % min
		if r != 0 {
			for j := uint32(2); j <= min; j++ {
				if (j*max)%min == 0 {
					out = j * max
					break
				}
			}
		} else {
			out = max
		}
	}
	return out
}

func IsDigit(r byte) bool {
	return '0' <= r && r <= '9'
}
