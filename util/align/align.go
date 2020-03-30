package align

// Backward aligns the ts to the previous ts that divides by the interval, even if it is already aligned
func Backward(ts uint32, interval uint32) uint32 {
	return ts - ((ts-1)%interval + 1)
}

// BackwardIfNotAligned aligns the ts to the previous ts that divides by the interval, except if it is already aligned
func BackwardIfNotAligned(ts uint32, interval uint32) uint32 {
	if ts%interval == 0 {
		return ts
	}
	return Backward(ts, interval)
}

// Forward aligns ts to the next timestamp that divides by the interval, even if it is already aligned
func Forward(ts, interval uint32) uint32 {
	remain := ts % interval
	return ts + interval - remain
}

// ForwardIfNotAligned aligns ts to the next timestamp that divides by the interval, except if it is already aligned
func ForwardIfNotAligned(ts, interval uint32) uint32 {
	if ts%interval == 0 {
		return ts
	}
	return Forward(ts, interval)
}
