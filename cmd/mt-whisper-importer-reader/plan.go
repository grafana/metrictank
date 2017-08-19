package main

import (
	"github.com/kisielk/whisper-go/whisper"
)

type conversion int8

type archive struct {
	idx        int
	timeRange  uint32
	conversion conversion // -1 decrease, 0 no action, 1 increase
	currSPP    uint32
	needSPP    uint32
}

type plan struct {
	archives []archive
	points   []whisper.Point
	whisper  *whisper.Whisper
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

func NewPlan(spp, nop uint32, w *whisper.Whisper) plan {
	ttl := spp * nop

	// find the largest archive that has at least the resolution we need
	startArchiveIdx := 0
	for idx, archive := range w.Header.ArchiveInfo {
		if archive.SecondsPerPoint > spp {
			break
		}
		startArchiveIdx = idx
	}

	// for how many seconds of range we've planned out
	plannedUpTo := uint32(0)
	res := plan{whisper: w}

	for idx, archive := range w.Header.Archives {
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

		a := archive{
			archive:   idx,
			timeRange: plannedUpTo,
			currSPP:   archive.SecondsPerPoint,
			needSPP:   spp,
		}

		if archive.SecondsPerPoint == spp {
			a.conversion = 0
		} else if archive.SecondsPerPoint < spp {
			a.conversion = -1
		} else {
			a.conversion = 1
		}

		res.archives = append(res.archive, a)
	}

	return res
}

func (c *plan) getAdjustedPoints() []whisper.Point {
	for _, archive := range c.archives {
		in := c.whisper.DumpArchive(archive.idx)
		c.whisper.Header.Archives[archive.idx].Offset
	}
}

// pretty print
/*func (ps *plans) String() string {
	var buffer bytes.Buffer
	for _, p := range *ps {
		buffer.WriteString(fmt.Sprintf(
			"arch:%d, seconds:%d-%d, resolution:%s\n", p.archive, p.timeFrom, p.TimeUntil, p.conversion.String(),
		))
	}
	return buffer.String()
}*/
