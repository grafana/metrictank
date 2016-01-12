package main

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint32) uint32 {
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
func lcm(vals []uint32) uint32 {
	out := vals[0]
	for i := 1; i < len(vals); i++ {
		a := max(uint32(vals[i]), out)
		b := min(uint32(vals[i]), out)
		r := a % b
		if r != 0 {
			for j := uint32(2); j <= b; j++ {
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
