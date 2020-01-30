package util

// AllCombinationsUint32 returns all combinations of the input
func AllCombinationsUint32(parts [][]uint32) (out [][]uint32) {

	// allocate a slice to host all combinations
	num := 1
	for _, part := range parts {
		num *= len(part)
	}
	out = make([][]uint32, 0, num)

	// will contain idx of which one to pick for each part
	indexes := make([]int, len(parts))

mainloop:
	for {
		// update indexes:
		// travel backwards. whenever we encounter an index that "overflowed"
		// reset it back to 0 and bump the previous one, until they are all maxed out
		for i := len(indexes) - 1; i >= 0; i-- {
			if indexes[i] >= len(parts[i]) {
				if i == 0 {
					break mainloop
				}
				indexes[i] = 0
				indexes[i-1]++
			}
		}

		combo := make([]uint32, len(parts))
		for i, part := range parts {
			combo[i] = part[indexes[i]]
		}
		out = append(out, combo)

		// always bump idx of the last one
		indexes[len(parts)-1]++
	}
	return out
}
