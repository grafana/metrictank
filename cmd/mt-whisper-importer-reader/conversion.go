package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/metrictank/mdata"
)

type conversion struct {
	whisper *whisper.Whisper
	method  string
}

func newConversion(w *whisper.Whisper) *conversion {
	conversion := conversion{whisper: w, method: shortAggMethodString(w.Header.Metadata.AggregationMethod)}
	return &conversion
}

func (c *conversion) getPoints(retIdx int, method string, spp, nop uint32) (map[string][]whisper.Point, error) {
	ttl := spp * nop
	res := make(map[string][]whisper.Point)

	// find smallest archive that still contains enough data to satisfy requested range
	largestArchiveIdx := len(c.whisper.Header.Archives) - 1
	for i := largestArchiveIdx; i >= 0; i-- {
		arch := c.whisper.Header.Archives[i]
		if arch.SecondsPerPoint*arch.Points < ttl {
			break
		}
		largestArchiveIdx = i
	}

	// find largest archive that still has higher resolution than requested
	smallestArchiveIdx := 0
	for i := 0; i < len(c.whisper.Header.Archives); i++ {
		arch := c.whisper.Header.Archives[i]
		if arch.SecondsPerPoint > spp {
			break
		}
		smallestArchiveIdx = i
	}

	for i := largestArchiveIdx; i > smallestArchiveIdx; i++ {
		in, err := c.whisper.DumpArchive(i)
		if err != nil {
			return res, err
		}
		adjustedPoints := make(map[string][]whisper.Point)
		arch := c.whisper.Header.Archives[i]
		if arch.SecondsPerPoint == spp {
			if retIdx == 0 || method != "avg" {
				adjustedPoints[method] = in
			} else {
				adjustedPoints["sum"] = in
				for _, c := range adjustedPoints["sum"] {
					adjustedPoints["cnt"] = append(adjustedPoints["cnt"], whisper.Point{
						Timestamp: c.Timestamp,
						Value:     1,
					})
				}
			}
		} else if arch.SecondsPerPoint > spp {
			if method != "avg" || retIdx == 0 {
				adjustedPoints[method] = incResolution(in, arch.SecondsPerPoint, spp)
			} else {
				adjustedPoints = incResolutionFakeAvg(in, arch.SecondsPerPoint, spp)
			}
		} else {
			if method != "avg" || retIdx == 0 {
				adjustedPoints[method] = decResolution(in, method, arch.SecondsPerPoint, spp)
			} else {
				//adjustedPoints = decResolutionFakeAvg(in, method, arch.SecondsPerPoint, spp)
			}
		}
	}

	return res, nil
}

func incResolution(points []whisper.Point, inRes, outRes uint32) []whisper.Point {
	pointCount := (points[len(points)-1].Timestamp - points[0].Timestamp) * outRes / inRes
	out := make([]whisper.Point, 0, pointCount)
	for _, inPoint := range points {
		if inPoint.Timestamp == 0 {
			continue
		}

		var rangeStart uint32
		if inPoint.Timestamp%outRes == 0 {
			rangeStart = inPoint.Timestamp
		} else {
			rangeStart = inPoint.Timestamp - (inPoint.Timestamp % outRes) + outRes
		}
		for ts := rangeStart; ts < inPoint.Timestamp+inRes; ts = ts + outRes {
			outPoint := inPoint
			outPoint.Timestamp = ts
			out = append(out, outPoint)
		}
	}
	return sortPoints(out)
}

// inreasing the resolution by just duplicating points to fill in empty data points
func incResolutionFakeAvg(points []whisper.Point, inRes, outRes uint32) map[string][]whisper.Point {
	out := make(map[string][]whisper.Point)
	for _, inPoint := range points {
		if inPoint.Timestamp == 0 {
			continue
		}

		var rangeStart uint32
		if inPoint.Timestamp%outRes == 0 {
			rangeStart = inPoint.Timestamp
		} else {
			rangeStart = inPoint.Timestamp - (inPoint.Timestamp % outRes) + outRes
		}

		var outPoints []whisper.Point
		for ts := rangeStart; ts < inPoint.Timestamp+inRes; ts = ts + outRes {
			outPoints = append(outPoints, whisper.Point{Timestamp: ts})
		}

		for _, outPoint := range outPoints {
			outPoint.Value = inPoint.Value / float64(len(outPoints))
			out["sum"] = append(out["sum"], outPoint)
			out["cnt"] = append(out["cnt"], whisper.Point{Timestamp: outPoint.Timestamp, Value: float64(1) / float64(len(outPoints))})
		}
	}
	return out
}

func decResolution(points []whisper.Point, aggMethod string, inRes, outRes uint32) []whisper.Point {
	agg := mdata.NewAggregation()
	out := make([]whisper.Point, 0)
	currentBoundary := uint32(0)

	flush := func() {
		values := agg.FlushAndReset()
		if values["cnt"] == 0 {
			return
		}

		out = append(out, whisper.Point{
			Timestamp: currentBoundary,
			Value:     values[aggMethod],
		})
	}

	for _, inPoint := range sortPoints(points) {
		if inPoint.Timestamp == 0 {
			continue
		}
		boundary := mdata.AggBoundary(inPoint.Timestamp, outRes)

		if boundary == currentBoundary {
			agg.Add(inPoint.Value)
			if inPoint.Timestamp == boundary {
				flush()
			}
		} else {
			flush()
			currentBoundary = boundary
			agg.Add(inPoint.Value)
		}
	}

	return out
}

func decResolutionFakeAvg(points []whisper.Point, inRes, outRes uint32) map[string][]whisper.Point {
	out := make(map[string][]whisper.Point)
	agg := mdata.NewAggregation()
	currentBoundary := uint32(0)

	flush := func() {
		values := agg.FlushAndReset()
		if values["cnt"] == 0 {
			return
		}

		out["sum"] = append(out["sum"], whisper.Point{
			Timestamp: currentBoundary,
			Value:     values["sum"],
		})
		out["cnt"] = append(out["cnt"], whisper.Point{
			Timestamp: currentBoundary,
			Value:     values["cnt"],
		})
	}

	for _, inPoint := range sortPoints(points) {
		if inPoint.Timestamp == 0 {
			continue
		}
		boundary := mdata.AggBoundary(inPoint.Timestamp, outRes)

		if boundary == currentBoundary {
			agg.Add(inPoint.Value)
			if inPoint.Timestamp == boundary {
				flush()
			}
		} else {
			flush()
			currentBoundary = boundary
			agg.Add(inPoint.Value)
		}
	}

	return out
}
