package chunk

import "fmt"

type SpanCode uint8

var ChunkSpans = [32]uint32{
	1,
	5,
	10,
	15,
	20,
	30,
	60,        // 1m
	90,        // 1.5m
	2 * 60,    // 2m
	3 * 60,    // 3m
	5 * 60,    // 5m
	10 * 60,   // 10m
	15 * 60,   // 15m
	20 * 60,   // 20m
	30 * 60,   // 30m
	45 * 60,   // 45m
	3600,      // 1h
	90 * 60,   // 1.5h
	2 * 3600,  // 2h
	150 * 60,  // 2.5h
	3 * 3600,  // 3h
	4 * 3600,  // 4h
	5 * 3600,  // 5h
	6 * 3600,  // 6h
	7 * 3600,  // 7h
	8 * 3600,  // 8h
	9 * 3600,  // 9h
	10 * 3600, // 10h
	12 * 3600, // 12h
	15 * 3600, // 15h
	18 * 3600, // 18h
	24 * 3600, // 24h
}

var RevChunkSpans = make(map[uint32]SpanCode, len(ChunkSpans))
var ErrUnknownChunkSpan = fmt.Errorf("Cannot determine span of chunk")

func init() {
	for k, v := range ChunkSpans {
		RevChunkSpans[v] = SpanCode(k)
	}
}

// SpanOfChunk takes a chunk and tries to determine its span.
// It returns an error if it failed to determine the span, this could fail
// either because the given chunk is invalid or because it has an old format
func SpanOfChunk(chunk []byte) (uint32, error) {
	if len(chunk) < 2 {
		return 0, ErrUnknownChunkSpan
	}

	if Format(chunk[0]) != FormatStandardGoTszWithSpan && Format(chunk[0]) != FormatGoTszLongWithSpan {
		return 0, ErrUnknownChunkSpan
	}

	if int(chunk[1]) >= len(ChunkSpans) {
		return 0, ErrUnknownChunkSpan
	}

	return ChunkSpans[SpanCode(chunk[1])], nil
}
