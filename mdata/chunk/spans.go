package chunk

type SpanCode uint8

const min = 60
const hour = 60 * min

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

func init() {
	for k, v := range ChunkSpans {
		RevChunkSpans[v] = SpanCode(k)
	}
}
