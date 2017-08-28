package main

import (
	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/metrictank/mdata"
)

type conversion struct {
	archives []whisper.ArchiveInfo
	points   map[int][]whisper.Point
	method   string
}

func newConversion(arch []whisper.ArchiveInfo, points map[int][]whisper.Point, method string) *conversion {
	return &conversion{archives: arch, points: points, method: method}
}

func (c *conversion) findSmallestLargestArchive(spp, nop uint32) (int, int) {
	// find smallest archive that still contains enough data to satisfy requested range
	largestArchiveIdx := len(c.archives) - 1
	for i := largestArchiveIdx; i >= 0; i-- {
		arch := c.archives[i]
		if arch.Points*arch.SecondsPerPoint < nop*spp {
			break
		}
		largestArchiveIdx = i
	}

	// find largest archive that still has higher resolution than requested
	smallestArchiveIdx := 0
	for i := 0; i < len(c.archives); i++ {
		arch := c.archives[i]
		if arch.SecondsPerPoint > spp {
			break
		}
		smallestArchiveIdx = i
	}

	return smallestArchiveIdx, largestArchiveIdx
}

// generates points according to specified parameters by finding and using the best archives as input
func (c *conversion) getPoints(retIdx int, spp, nop uint32) map[string][]whisper.Point {
	res := make(map[string][]whisper.Point)

	if len(c.points) == 0 {
		return res
	}

	// figure out the range of archives that make sense to use for the requested specs
	smallestArchiveIdx, largestArchiveIdx := c.findSmallestLargestArchive(spp, nop)
	rawRes := c.archives[0].SecondsPerPoint

	adjustedPoints := make(map[string]map[uint32]float64)
	if retIdx > 0 && c.method == "avg" {
		adjustedPoints["cnt"] = make(map[uint32]float64)
		adjustedPoints["sum"] = make(map[uint32]float64)
	} else {
		adjustedPoints[c.method] = make(map[uint32]float64)
	}

	// Out of the input archives that we'll use, start with the lowest resolution one by converting
	// it to the requested resolution and filling the resulting points into adjustedPoints.
	// Then continue with archives of increasing resolutions while overwriting the generated points
	// of previous ones.
	for i := largestArchiveIdx; i >= smallestArchiveIdx; i-- {
		in := c.points[i]
		arch := c.archives[i]
		if arch.SecondsPerPoint == spp {
			if retIdx == 0 || c.method != "avg" {
				for _, p := range in {
					adjustedPoints[c.method][p.Timestamp] = p.Value
				}
			} else {
				for _, p := range in {
					adjustedPoints["sum"][p.Timestamp] = p.Value
					adjustedPoints["cnt"][p.Timestamp] = 1
				}
			}
		} else if arch.SecondsPerPoint > spp {
			method := c.method
			if method == "avg" && retIdx > 0 {
				method = "fakeavg"
			}
			for m, points := range incResolution(in, method, arch.SecondsPerPoint, spp, rawRes) {
				for _, p := range points {
					adjustedPoints[m][p.Timestamp] = p.Value
				}
			}
		} else {
			methods := []string{c.method}
			if c.method == "avg" && retIdx > 0 {
				methods = []string{"sum", "cnt"}
			}
			for m, points := range decResolution(in, methods, arch.SecondsPerPoint, spp) {
				for _, p := range points {
					adjustedPoints[m][p.Timestamp] = p.Value
				}
			}
		}

	}

	// merge the results that are keyed by timestamp into a slice of points
	for m, p := range adjustedPoints {
		for t, v := range p {
			res[m] = append(res[m], whisper.Point{Timestamp: t, Value: v})
		}
		res[m] = sortPoints(res[m])
	}

	return res
}

// increase resolution of given points according to defined specs
func incResolution(points []whisper.Point, method string, inRes, outRes, rawRes uint32) map[string][]whisper.Point {
	out := make(map[string][]whisper.Point)
	aggFactor := float64(outRes) / float64(rawRes)
	for _, inPoint := range points {
		if inPoint.Timestamp == 0 {
			continue
		}

		rangeEnd := inPoint.Timestamp - (inPoint.Timestamp % outRes)

		var outPoints []whisper.Point
		for ts := rangeEnd; ts > inPoint.Timestamp-inRes; ts = ts - outRes {
			outPoints = append(outPoints, whisper.Point{Timestamp: ts})
		}

		for _, outPoint := range outPoints {
			if method == "cnt" || method == "sum" {
				outPoint.Value = inPoint.Value / float64(len(outPoints))
			} else if method == "fakeavg" {
				outPoint.Value = inPoint.Value * aggFactor
			} else {
				outPoint.Value = inPoint.Value
			}

			if method == "fakeavg" {
				out["sum"] = append(out["sum"], outPoint)
				out["cnt"] = append(out["cnt"], whisper.Point{Timestamp: outPoint.Timestamp, Value: aggFactor})
			} else {
				out[method] = append(out[method], outPoint)
			}
		}
	}
	for m := range out {
		out[m] = sortPoints(out[m])
	}
	return out
}

func decResolution(points []whisper.Point, methods []string, inRes, outRes uint32) map[string][]whisper.Point {
	out := make(map[string][]whisper.Point)
	agg := mdata.NewAggregation()
	currentBoundary := uint32(0)

	flush := func() {
		if agg.Cnt == 0 {
			return
		}

		for _, method := range methods {
			var value float64
			switch method {
			case "min":
				value = agg.Min
			case "max":
				value = agg.Max
			case "sum":
				value = agg.Sum
			case "cnt":
				value = agg.Cnt
			case "lst":
				value = agg.Lst
			case "avg":
				value = agg.Sum / agg.Cnt
			default:
				return
			}
			out[method] = append(out[method], whisper.Point{
				Timestamp: currentBoundary,
				Value:     value,
			})
		}
		agg.Reset()
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
