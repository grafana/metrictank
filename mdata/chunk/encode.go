package chunk

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// encode is a helper function to encode a chunk of data into various formats
func encode(span uint32, format Format, data []byte) []byte {
	switch format {
	case FormatStandardGoTszWithSpan, FormatGoTszLongWithSpan:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, format)

		spanCode, ok := RevChunkSpans[span]
		if !ok {
			// it's probably better to panic than to persist the chunk with a wrong length
			panic(fmt.Sprintf("Chunk span invalid: %d", span))
		}
		binary.Write(buf, binary.LittleEndian, spanCode)
		buf.Write(data)
		return buf.Bytes()
	}
	return nil
}
