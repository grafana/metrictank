package main

import (
	"github.com/grafana/metrictank/mdata"
	"github.com/kisielk/whisper-go/whisper"
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

	// find largest archive that still has a higher or equal resolution than requested
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
	if retIdx > 0 && c.method == "avg" || c.method == "sum" {
		adjustedPoints["cnt"] = make(map[uint32]float64)
		adjustedPoints["sum"] = make(map[uint32]float64)
	} else {
		adjustedPoints[c.method] = make(map[uint32]float64)
	}

	method := c.method
	if method == "avg" && retIdx > 0 {
		method = "fakeavg"
	}

	// Out of the input archives that we'll use, start with the lowest resolution one by converting
	// it to the requested resolution and filling the resulting points into adjustedPoints.
	// Then continue with archives of increasing resolutions while overwriting the generated points
	// of previous ones.
	for i := largestArchiveIdx; i >= smallestArchiveIdx; i-- {
		in := c.points[i]
		arch := c.archives[i]
		if arch.SecondsPerPoint == spp {
			rawFactor := float64(spp) / float64(rawRes)
			if retIdx == 0 || c.method != "avg" {
				for _, p := range in {
					if p.Timestamp > uint32(*importUpTo) || p.Timestamp < uint32(*importAfter) {
						continue
					}
					adjustedPoints[c.method][p.Timestamp] = p.Value
					if c.method == "sum" {
						adjustedPoints["cnt"][p.Timestamp] = rawFactor
					}
				}
			} else {
				for _, p := range in {
					if p.Timestamp > uint32(*importUpTo) || p.Timestamp < uint32(*importAfter) {
						continue
					}
					adjustedPoints["sum"][p.Timestamp] = p.Value * rawFactor
					adjustedPoints["cnt"][p.Timestamp] = rawFactor
				}
			}
		} else if arch.SecondsPerPoint > spp {
			for m, points := range incResolution(in, method, arch.SecondsPerPoint, spp, rawRes) {
				for _, p := range points {
					adjustedPoints[m][p.Timestamp] = p.Value
				}
			}
		} else {
			for m, points := range decResolution(in, method, arch.SecondsPerPoint, spp, rawRes) {
				for _, p := range points {
					adjustedPoints[m][p.Timestamp] = p.Value
				}
			}
		}

	}

	// merge the results that are keyed by timestamp into a slice of points
	for m, p := range adjustedPoints {
		for t, v := range p {
			if t <= uint32(*importUpTo) && t >= uint32(*importAfter) {
				res[m] = append(res[m], whisper.Point{Timestamp: t, Value: v})
			}
		}
		res[m] = sortPoints(res[m])

		// if the resolution of data had to be increased it's possible that we
		// get a little more historic data than necessary, so we chop off the
		// older data that's not needed
		if uint32(len(res[m])) > nop {
			res[m] = res[m][uint32(len(res[m]))-nop:]
		}
	}

	return res
}

// increase resolution of given points according to defined specs by generating
// additional datapoints to bridge the gaps between the given points. depending
// on what aggregation method is specified, those datapoints may be generated in
// slightly different ways.
func incResolution(points []whisper.Point, method string, inRes, outRes, rawRes uint32) map[string][]whisper.Point {
	out := make(map[string][]whisper.Point)
	resFactor := float64(outRes) / float64(rawRes)
	for _, inPoint := range points {
		if inPoint.Timestamp == 0 {
			continue
		}

		// inPoints are guaranteed to be quantized by whisper
		// outRes is < inRes, otherwise this function should never be called
		// rangeEnd is the the TS of the last datapoint that will be generated based on inPoint
		rangeEnd := inPoint.Timestamp - (inPoint.Timestamp % outRes)

		// generate datapoints based on inPoint in reverse order
		var outPoints []whisper.Point
		for ts := rangeEnd; ts > inPoint.Timestamp-inRes; ts = ts - outRes {
			if ts > uint32(*importUpTo) || ts < uint32(*importAfter) {
				continue
			}
			outPoints = append(outPoints, whisper.Point{Timestamp: ts})
		}

		for _, outPoint := range outPoints {
			if method == "sum" {
				outPoint.Value = inPoint.Value / float64(len(outPoints))
				out["sum"] = append(out["sum"], outPoint)
				out["cnt"] = append(out["cnt"], whisper.Point{Timestamp: outPoint.Timestamp, Value: resFactor})
			} else if method == "fakeavg" {
				outPoint.Value = inPoint.Value * resFactor
				out["sum"] = append(out["sum"], outPoint)
				out["cnt"] = append(out["cnt"], whisper.Point{Timestamp: outPoint.Timestamp, Value: resFactor})
			} else {
				outPoint.Value = inPoint.Value
				out[method] = append(out[method], outPoint)
			}
		}
	}
	for m := range out {
		out[m] = sortPoints(out[m])
	}
	return out
}

// decreases the resolution of given points by using the aggregation method specified
// in the second argument. emulates the way metrictank aggregates data when it generates
// rollups of the raw data.
func decResolution(points []whisper.Point, method string, inRes, outRes, rawRes uint32) map[string][]whisper.Point {
	out := make(map[string][]whisper.Point)
	agg := mdata.NewAggregation()
	currentBoundary := uint32(0)

	flush := func() {
		if agg.Cnt == 0 {
			return
		}

		var value float64
		switch method {
		case "min":
			value = agg.Min
		case "max":
			value = agg.Max
		case "lst":
			value = agg.Lst
		case "avg":
			value = agg.Sum / agg.Cnt
		case "sum":
			out["cnt"] = append(out["cnt"], whisper.Point{
				Timestamp: currentBoundary,
				Value:     agg.Cnt * float64(inRes) / float64(rawRes),
			})
			out["sum"] = append(out["sum"], whisper.Point{
				Timestamp: currentBoundary,
				Value:     agg.Sum,
			})
			agg.Reset()
			return
		case "fakeavg":
			cnt := agg.Cnt * float64(inRes) / float64(rawRes)
			out["cnt"] = append(out["cnt"], whisper.Point{
				Timestamp: currentBoundary,
				Value:     cnt,
			})
			out["sum"] = append(out["sum"], whisper.Point{
				Timestamp: currentBoundary,
				Value:     (agg.Sum / agg.Cnt) * cnt,
			})
			agg.Reset()
			return
		default:
			return
		}
		out[method] = append(out[method], whisper.Point{
			Timestamp: currentBoundary,
			Value:     value,
		})
		agg.Reset()
	}

	for _, inPoint := range sortPoints(points) {
		if inPoint.Timestamp == 0 {
			continue
		}
		boundary := mdata.AggBoundary(inPoint.Timestamp, outRes)
		if boundary > uint32(*importUpTo) {
			break
		}
		if boundary < uint32(*importAfter) {
			continue
		}

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
