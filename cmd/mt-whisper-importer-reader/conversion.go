package main

import (
	"errors"

	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/metrictank/mdata"
)

var (
	errUnknownArchive = errors.New("Archive not found")
	errNoData         = errors.New("Whisper file appears to have no data")
)

type conversion struct {
	archives []whisper.ArchiveInfo
	points   map[int][]whisper.Point
	method   string
}

func newConversion(arch []whisper.ArchiveInfo, points map[int][]whisper.Point, method string) *conversion {
	conversion := conversion{archives: arch, points: points, method: method}
	return &conversion
}

func (c *conversion) findSmallestLargestArchive(ttl, spp uint32) (int, int) {
	// find smallest archive that still contains enough data to satisfy requested range
	largestArchiveIdx := len(c.archives) - 1
	for i := largestArchiveIdx; i >= 0; i-- {
		arch := c.archives[i]
		if arch.SecondsPerPoint*arch.Points < ttl {
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

func (c *conversion) getPoints(retIdx int, spp, nop uint32) (map[string][]whisper.Point, error) {
	ttl := spp * nop
	res := make(map[string][]whisper.Point)

	if len(c.points) == 0 {
		return res, errNoData
	}

	smallestArchiveIdx, largestArchiveIdx := c.findSmallestLargestArchive(ttl, spp)

	adjustedPoints := make(map[string]map[uint32]float64)
	if retIdx > 0 && c.method == "avg" {
		adjustedPoints["cnt"] = make(map[uint32]float64)
		adjustedPoints["sum"] = make(map[uint32]float64)
	} else {
		adjustedPoints[c.method] = make(map[uint32]float64)
	}

	for i := largestArchiveIdx; i >= smallestArchiveIdx; i-- {
		in, ok := c.points[i]
		if !ok {
			return res, errUnknownArchive
		}
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
			if c.method != "avg" || retIdx == 0 {
				for _, p := range incResolution(in, c.method, arch.SecondsPerPoint, spp) {
					adjustedPoints[c.method][p.Timestamp] = p.Value
				}
			} else {
				for m, points := range incResolutionFakeAvg(in, arch.SecondsPerPoint, spp) {
					for _, p := range points {
						adjustedPoints[m][p.Timestamp] = p.Value
					}
				}
			}
		} else {
			if c.method != "avg" || retIdx == 0 {
				for _, p := range decResolution(in, c.method, arch.SecondsPerPoint, spp) {
					adjustedPoints[c.method][p.Timestamp] = p.Value
				}
			} else {
				for m, points := range decResolutionFakeAvg(in, arch.SecondsPerPoint, spp) {
					for _, p := range points {
						adjustedPoints[m][p.Timestamp] = p.Value
					}
				}
			}
		}

	}
	for m, p := range adjustedPoints {
		for t, v := range p {
			res[m] = append(res[m], whisper.Point{Timestamp: t, Value: v})
		}
		res[m] = sortPoints(res[m])
	}

	return res, nil
}

func incResolution(points []whisper.Point, method string, inRes, outRes uint32) []whisper.Point {
	var out []whisper.Point
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
			if method == "sum" || method == "cnt" {
				outPoint.Value = inPoint.Value / float64(len(outPoints))
			} else {
				outPoint.Value = inPoint.Value
			}
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

		rangeEnd := inPoint.Timestamp - (inPoint.Timestamp % outRes)

		var outPoints []whisper.Point
		for ts := rangeEnd; ts > inPoint.Timestamp-inRes; ts = ts - outRes {
			outPoints = append(outPoints, whisper.Point{Timestamp: ts})
		}

		for _, outPoint := range outPoints {
			outPoint.Value = inPoint.Value / float64(len(outPoints))
			out["sum"] = append(out["sum"], outPoint)
			out["cnt"] = append(out["cnt"], whisper.Point{Timestamp: outPoint.Timestamp, Value: float64(1) / float64(len(outPoints))})
		}
	}
	out["sum"] = sortPoints(out["sum"])
	out["cnt"] = sortPoints(out["cnt"])
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
