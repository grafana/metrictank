package align

// Backward aligns the ts to the previous ts that divides by the interval, even if it is already aligned
func Backward(ts uint32, interval uint32) uint32 {
	return ts - ((ts-1)%interval + 1)
}

// BackwardIfNotAligned aligns the ts to the previous ts that divides by the interval, except if it is already aligned
func BackwardIfNotAligned(ts uint32, interval uint32) uint32 {
	return ts - ts%interval
}

// Forward aligns ts to the next timestamp that divides by the interval, even if it is already aligned
func Forward(ts, interval uint32) uint32 {
	remain := ts % interval
	return ts + interval - remain
}

// ForwardIfNotAligned aligns ts to the next timestamp that divides by the interval, except if it is already aligned
func ForwardIfNotAligned(ts, interval uint32) uint32 {
	base := ts + interval - 1
	return base - base%interval
}
