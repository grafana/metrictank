package archives

import (
	"github.com/grafana/metrictank/internal/mdata"
	"github.com/grafana/metrictank/pkg/conf"
)

// Archive is like conf.Retention except:
// * for archive 0, the "interval" is the raw interval of the metric, if it differs from what the retention specifies
// * only contains relevant fields for planning purposes
type Archive struct {
	Interval uint32
	TTL      uint32
	Ready    uint32 // ready for reads for data as of this timestamp (or as of now-TTL, whichever is highest)
}

// Valid returns whether the given Archive has been ready long enough (wrt from), and has a sufficient retention (wrt ttl)
func (a Archive) Valid(from, ttl uint32) bool {
	return a.Ready <= from && a.TTL >= ttl
}

func NewArchive(ret conf.Retention) Archive {
	return Archive{
		Interval: uint32(ret.SecondsPerPoint),
		TTL:      uint32(ret.MaxRetention()),
		Ready:    ret.Ready,
	}
}

type Archives []Archive

func NewArchives(rawInterval uint32, schemaID uint16) Archives {

	rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets

	archives := make(Archives, len(rets))
	for i, ret := range rets {
		archives[i] = NewArchive(ret)
	}
	archives[0].Interval = rawInterval

	return archives
}
