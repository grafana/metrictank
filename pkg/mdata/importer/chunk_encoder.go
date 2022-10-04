package importer

import (
	"fmt"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/kisielk/whisper-go/whisper"
)

func encodeChunksFromPoints(points []whisper.Point, intervalIn, chunkSpan uint32, writeUnfinishedChunks bool) []*chunk.Chunk {
	var point whisper.Point
	var t0, prevT0 uint32
	var c *chunk.Chunk
	var encodedChunks []*chunk.Chunk

	for _, point = range points {
		// this shouldn't happen, but if it would we better catch it here because Metrictank wouldn't handle it well:
		// https://github.com/grafana/metrictank/blob/f1868cccfb92fc82cd853914af958f6d187c5f74/mdata/aggmetric.go#L378
		if point.Timestamp == 0 {
			continue
		}

		t0 = point.Timestamp - (point.Timestamp % chunkSpan)
		if prevT0 == 0 {
			c = chunk.New(t0)
			prevT0 = t0
		} else if prevT0 != t0 {
			c.Finish()
			encodedChunks = append(encodedChunks, c)

			c = chunk.New(t0)
			prevT0 = t0
		}

		err := c.Push(point.Timestamp, point.Value)
		if err != nil {
			panic(fmt.Sprintf("ERROR: Failed to push value into chunk at t0 %d: %q", t0, err))
		}
	}

	// if the last written point was also the last one of the current chunk,
	// or if writeUnfinishedChunks is on, we close the chunk and push it
	if point.Timestamp == t0+chunkSpan-intervalIn || writeUnfinishedChunks {
		c.Finish()
		encodedChunks = append(encodedChunks, c)
	}

	return encodedChunks
}
