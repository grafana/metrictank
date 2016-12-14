package chunk

// this exists so that we can later add more formats, perhaps for int/uint/float32/bool specific optimisations, or other encoding/decoding/compression algorithms
// and have an easy time distinguishing the binary blobs

//go:generate stringer -type=Format

type Format uint8

// identifier of message format
const (
	FormatStandardGoTsz Format = iota
	FormatStandardGoTszWithSpan
)
