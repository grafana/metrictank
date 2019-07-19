package idx

import (
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/stats"
)

var (
	safeArchiveCount = stats.NewGauge32("idx.archive_pool")
)

var safeArchivePool = sync.Pool{
	New: func() interface{} {
		return new(Archive)
	},
}

var metricDefinitionPool = sync.Pool{
	New: func() interface{} {
		return new(MetricDefinition)
	},
}

// NewSafeArchive takes a pointer to an Archive and creates a new
// safe copy of it. A safe copy in this context means that when
// accessing the copy one does not need to worry about atomics or
// the string interning.
// It is important that SafeArchive.ReleaseSafeArchive() gets called
// before it goes out of scope to return its memory back to the pool.
func NewSafeArchive(archive *Archive) *Archive {
	safeArchive := safeArchivePool.Get().(*Archive)
	safeArchive.SchemaId = archive.SchemaId
	safeArchive.AggId = archive.AggId
	safeArchive.IrId = archive.IrId
	safeArchive.LastSave = atomic.LoadUint32(&archive.LastSave)

	InternIncMetricDefinitionRefCounts(*archive.MetricDefinition)
	metricDefinition := metricDefinitionPool.Get().(*MetricDefinition)
	*metricDefinition = *(archive.MetricDefinition.Clone())
	safeArchive.MetricDefinition = metricDefinition

	safeArchiveCount.AddUint32(1)

	return safeArchive
}

func (s *Archive) ReleaseSafeArchive() {
	InternReleaseMetricDefinition(*s.MetricDefinition)
	metricDefinitionPool.Put(s.MetricDefinition)
	safeArchivePool.Put(s)
	safeArchiveCount.DecUint32(1)
}
