package importer

import (
	"fmt"
	"sort"

	"github.com/grafana/metrictank/mdata"
	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/schema"
)

type converter struct {
	archives []whisper.ArchiveInfo
	points   map[int][]whisper.Point
	method   schema.Method
	from     uint32
	until    uint32
}

const fakeAvg schema.Method = 255

func newConverter(arch []whisper.ArchiveInfo, points map[int][]whisper.Point, method schema.Method, from, until uint32) *converter {
	return &converter{archives: arch, points: points, method: method, from: from, until: until}
}

// generates points according to specified parameters by finding and using the best archives as input
func (c *converter) getPoints(retIdx int, spp, nop uint32) map[schema.Method][]whisper.Point {
	res := make(map[schema.Method][]whisper.Point)

	if len(c.points) == 0 {
		return res
	}

	// figure out the range of archives that make sense to use for the requested specs
	smallestArchiveIdx, largestArchiveIdx := c.findSmallestLargestArchive(spp, nop)
	rawRes := c.archives[0].SecondsPerPoint

	adjustedPoints := make(map[schema.Method]map[uint32]float64)
	if retIdx > 0 && c.method == schema.Avg || c.method == schema.Sum {
		adjustedPoints[schema.Cnt] = make(map[uint32]float64)
		adjustedPoints[schema.Sum] = make(map[uint32]float64)
	} else {
		adjustedPoints[c.method] = make(map[uint32]float64)
	}

	method := c.method
	if c.method == schema.Avg && retIdx > 0 {
		method = fakeAvg
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
			if retIdx == 0 || c.method != schema.Avg {
				for _, p := range in {
					if p.Timestamp > c.until || p.Timestamp < c.from {
						continue
					}
					adjustedPoints[c.method][p.Timestamp] = p.Value
					if c.method == schema.Sum {
						adjustedPoints[schema.Cnt][p.Timestamp] = rawFactor
					}
				}
			} else {
				for _, p := range in {
					if p.Timestamp > c.until || p.Timestamp < c.from {
						continue
					}
					adjustedPoints[schema.Sum][p.Timestamp] = p.Value * rawFactor
					adjustedPoints[schema.Cnt][p.Timestamp] = rawFactor
				}
			}
		} else if arch.SecondsPerPoint > spp {
			for m, points := range incResolution(in, method, arch.SecondsPerPoint, spp, rawRes, c.from, c.until) {
				for _, p := range points {
					adjustedPoints[m][p.Timestamp] = p.Value
				}
			}
		} else {
			for m, points := range decResolution(in, method, arch.SecondsPerPoint, spp, rawRes, c.from, c.until) {
				for _, p := range points {
					adjustedPoints[m][p.Timestamp] = p.Value
				}
			}
		}
	}

	// merge the results that are keyed by timestamp into a slice of points
	for m, p := range adjustedPoints {
		for t, v := range p {
			if t <= c.until && t >= c.from {
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

func (c *converter) findSmallestLargestArchive(spp, nop uint32) (int, int) {
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

// increase resolution of given points according to defined specs by generating
// additional datapoints to bridge the gaps between the given points. depending
// on what aggregation method is specified, those datapoints may be generated in
// slightly different ways.
func incResolution(points []whisper.Point, method schema.Method, inRes, outRes, rawRes, from, until uint32) map[schema.Method][]whisper.Point {
	out := make(map[schema.Method][]whisper.Point)
	resFactor := float64(outRes) / float64(rawRes)
	for _, inPoint := range points {
		if inPoint.Timestamp == 0 {
			continue
		}

		// inPoints are guaranteed to be quantized by whisper
		// outRes is < inRes, otherwise this function should never be called
		// rangeEnd is the TS of the last datapoint that will be generated
		// based on inPoint
		rangeEnd := inPoint.Timestamp - (inPoint.Timestamp % outRes)

		// generate datapoints based on inPoint in reverse order
		var outPoints []whisper.Point
		for ts := rangeEnd; ts > inPoint.Timestamp-inRes; ts = ts - outRes {
			if ts > until || ts < from {
				continue
			}
			outPoints = append(outPoints, whisper.Point{Timestamp: ts})
		}

		for _, outPoint := range outPoints {
			if method == schema.Sum {
				outPoint.Value = inPoint.Value / float64(len(outPoints))
				out[schema.Sum] = append(out[schema.Sum], outPoint)
				out[schema.Cnt] = append(out[schema.Cnt], whisper.Point{Timestamp: outPoint.Timestamp, Value: resFactor})
			} else if method == fakeAvg {
				outPoint.Value = inPoint.Value * resFactor
				out[schema.Sum] = append(out[schema.Sum], outPoint)
				out[schema.Cnt] = append(out[schema.Cnt], whisper.Point{Timestamp: outPoint.Timestamp, Value: resFactor})
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
func decResolution(points []whisper.Point, method schema.Method, inRes, outRes, rawRes, from, until uint32) map[schema.Method][]whisper.Point {
	out := make(map[schema.Method][]whisper.Point)
	agg := mdata.NewAggregation()
	currentBoundary := uint32(0)

	flush := func() {
		if agg.Cnt == 0 {
			return
		}

		var value float64
		switch method {
		case schema.Min:
			value = agg.Min
		case schema.Max:
			value = agg.Max
		case schema.Lst:
			value = agg.Lst
		case schema.Avg:
			value = agg.Sum / agg.Cnt
		case schema.Sum:
			out[schema.Cnt] = append(out[schema.Cnt], whisper.Point{
				Timestamp: currentBoundary,
				Value:     agg.Cnt * float64(inRes) / float64(rawRes),
			})
			out[schema.Sum] = append(out[schema.Sum], whisper.Point{
				Timestamp: currentBoundary,
				Value:     agg.Sum,
			})
			agg.Reset()
			return
		case fakeAvg:
			cnt := agg.Cnt * float64(inRes) / float64(rawRes)
			out[schema.Cnt] = append(out[schema.Cnt], whisper.Point{
				Timestamp: currentBoundary,
				Value:     cnt,
			})
			out[schema.Sum] = append(out[schema.Sum], whisper.Point{
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
		if boundary > until {
			break
		}
		if boundary < from {
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

// pointSorter sorts points by timestamp
type pointSorter []whisper.Point

func (a pointSorter) Len() int           { return len(a) }
func (a pointSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a pointSorter) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

// the whisper archives are organized like a ringbuffer. since we need to
// insert the points into the chunks in order we first need to sort them
func sortPoints(points pointSorter) pointSorter {
	sort.Sort(points)
	return points
}

func convertWhisperMethod(whisperMethod whisper.AggregationMethod) (schema.Method, error) {
	switch whisperMethod {
	case whisper.AggregationAverage:
		return schema.Avg, nil
	case whisper.AggregationSum:
		return schema.Sum, nil
	case whisper.AggregationLast:
		return schema.Lst, nil
	case whisper.AggregationMax:
		return schema.Max, nil
	case whisper.AggregationMin:
		return schema.Min, nil
	default:
		return 0, fmt.Errorf("Unknown whisper method: %d", whisperMethod)
	}
}
