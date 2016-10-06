package schema

// Reslice reslices a slice into smaller slices of the given max size.
func Reslice(in []*MetricData, size int) [][]*MetricData {
	numSubSlices := len(in) / size
	if len(in)%size > 0 {
		numSubSlices += 1
	}
	out := make([][]*MetricData, numSubSlices)
	for i := 0; i < numSubSlices; i++ {
		start := i * size
		end := (i + 1) * size
		if end > len(in) {
			out[i] = in[start:]
		} else {
			out[i] = in[start:end]
		}
	}
	return out
}
