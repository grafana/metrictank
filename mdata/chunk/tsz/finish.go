package tsz

// see end-of-stream marker information in devdocs/chunk-format.md

// finishV1 writes a v1 end-of-stream record to the bitstream
func finishV1(w *bstream) {
	w.writeBits(0x0f, 4)
	w.writeBits(0xffffffff, 32)
	w.writeBit(zero)
}

// finishV2 writes a a v1 end-of-stream record to the bitstream
func finishV2(w *bstream) {
	w.writeBits(0x1f, 5)
}
