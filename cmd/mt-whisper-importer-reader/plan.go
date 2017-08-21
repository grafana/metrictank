package main

import (
//"github.com/kisielk/whisper-go/whisper"
)

/*
type conversion int8

type inputArchive struct {
	idx        int
	conversion conversion // -1 decrease, 0 no action, 1 increase
	timeRange  uint32
}

type retention struct {
	inputs          []inputArchive
	secondsPerPoint uint32
	numberOfPoints  uint32
}

type plan struct {
	retentions []retention
	whisper    *whisper.Whisper
}

func (c conversion) String() string {
	if c == 0 {
		return "same"
	}
	if c == -1 {
		return "dec"
	}
	return "inc"
}

func newPlan(w *whisper.Whisper) plan {
	return plan{whisper: w}
}

func (p *plan) getPoints(retIdx int, method string, spp, nop uint32) map[string][]whisper.Point {
	ttl := spp * nop
	res := make(map[string][]whisper.Point)

	// find smallest archive that still contains enough data to satisfy requested range
	largestArchiveIdx := len(plan.whisper.Header.Archives) - 1
	for i := largestArchiveIdx; i >= 0; i-- {
		arch := plan.whisper.Header.Archives[i]
		if arch.SecondsPerPoint*arch.NumberOfPoints < ttl {
			break
		}
		largestArchiveIdx = i
	}

	// find largest archive that still has higher resolution than requested
	smallestArchiveIdx := 0
	for i := 0; i < len(plan.whisper.Header.Archives); i++ {
		arch := plan.whisper.Header.Archives[i]
		if arch.SecondsPerPoint > spp {
			break
		}
		smallestArchiveIdx = i
	}

	for i := largestArchiveIdx; i > smallestArchiveIdx; i++ {
		in := plan.whisper.DumpArchive(i)
		adjustedPoints := make(map[string][]whisper.Point)
		arch := plan.whisper.Header.Archives[i]
		if arch.SecondsPerPoint == spp {
			if retIdx == 0 || method != "avg" {
				adjustedPoints[method] = in
			} else {
				adjustedPoints["sum"] = in
				for _, p := range adjustedPoints["sum"] {
					adjustedPoints["cnt"] = whisper.Point{
						Timestamp: p.Timestamp,
						Value:     1,
					}
				}
			}
		} else if arch.SecondsPerPoint > spp {
			if method != "avg" || retIdx == 0 {
				adjustedPoints[method] = incResolution(in, arch.SecondsPerPoint, spp)
			} else {
				adjustedPoints = incResolutionFakeAvg(in, arch.SecondsPerPoint, spp)
			}
		} else {
			adjustedPoints = decResolution(in, method, arch.SecondsPerPoint, spp)
		}
	}

	return res
}

	// find the largest archive that has at least the resolution we need
	startArchiveIdx := 0
	for idx, archive := range p.whisper.Header.Archives {
		if archive.SecondsPerPoint > spp {
			break
		}
		startArchiveIdx = idx
	}

	var points []whisper.Point
	var covered uint32

	for idx, archive := range plan.whisper.Header.Archives {
		if idx < startArchiveIdx {
			continue
		}

		if plannedUpTo >= ttl {
			// we've planned enough to cover the whole ttl we're looking for
			return res
		}

		plannedUpTo = archive.SecondsPerPoint * archive.Points
		if ttl < plannedUpTo {
			plannedUpTo = ttl
		}

		a := inputArchive{
			archive:   idx,
			timeRange: plannedUpTo,
		}

		if archive.SecondsPerPoint == spp {
			a.conversion = 0
		} else if archive.SecondsPerPoint < spp {
			a.conversion = -1
		} else {
			a.conversion = 1
		}

		res.inputs = append(res.inputs, a)
	}

func (p *plan) addRetention(spp, nop uint32) {
	retention := retention{
		secondsPerPoint: spp,
		numberOfPoints:  nop,
	}

	ttl := spp * nop

	// find the largest archive that has at least the resolution we need
	startArchiveIdx := 0
	for idx, archive := range p.whisper.Header.Archives {
		if archive.SecondsPerPoint > spp {
			break
		}
		startArchiveIdx = idx
	}

	// for how many seconds of range we've planned out
	plannedUpTo := uint32(0)

	for idx, archive := range plan.whisper.Header.Archives {
		if idx < startArchiveIdx {
			continue
		}

		if plannedUpTo >= ttl {
			// we've planned enough to cover the whole ttl we're looking for
			return res
		}

		plannedUpTo = archive.SecondsPerPoint * archive.Points
		if ttl < plannedUpTo {
			plannedUpTo = ttl
		}

		a := inputArchive{
			archive:   idx,
			timeRange: plannedUpTo,
		}

		if archive.SecondsPerPoint == spp {
			a.conversion = 0
		} else if archive.SecondsPerPoint < spp {
			a.conversion = -1
		} else {
			a.conversion = 1
		}

		res.inputs = append(res.inputs, a)
	}
}

func (c *plan) getAdjustedPoints() []whisper.Point {
	for _, archive := range c.archives {
		in := c.whisper.DumpArchive(archive.idx)
		c.whisper.Header.Archives[archive.idx].Offset
	}
}

// pretty print
func (ps *plans) String() string {
	var buffer bytes.Buffer
	for _, p := range *ps {
		buffer.WriteString(fmt.Sprintf(
			"arch:%d, seconds:%d-%d, resolution:%s\n", p.archive, p.timeFrom, p.TimeUntil, p.conversion.String(),
		))
	}
	return buffer.String()
}
*/
