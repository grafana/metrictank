package idx

import (
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/stats"
	"github.com/raintank/schema"
)

var archiveInternedCount = stats.NewGauge32("idx.archive_interned_pool")

var archiveInternedPool = sync.Pool{
	New: func() interface{} {
		return new(ArchiveInterned)
	},
}

type Archive struct {
	schema.MetricDefinition
	SchemaId uint16 // index in mdata.schemas (not persisted)
	AggId    uint16 // index in mdata.aggregations (not persisted)
	IrId     uint16 // index in mdata.indexrules (not persisted)
	LastSave uint32 // last time the metricDefinition was saved to a backend store (cassandra)
}

// used primarily by tests, for convenience
func NewArchiveBare(name string) Archive {
	arc := Archive{}
	arc.Name = name
	return arc
}

type ArchiveInterned struct {
	*MetricDefinitionInterned
	SchemaId uint16 // index in mdata.schemas (not persisted)
	AggId    uint16 // index in mdata.aggregations (not persisted)
	IrId     uint16 // index in mdata.indexrules (not persisted)
	LastSave uint32 // last time the metricDefinition was saved to a backend store (cassandra)
}

func (a *ArchiveInterned) GetArchive() Archive {
	return Archive{
		a.MetricDefinitionInterned.ConvertToSchemaMd(),
		a.SchemaId,
		a.AggId,
		a.IrId,
		atomic.LoadUint32(&a.LastSave),
	}
}

// CloneInterned() creates a new safe copy of the interned
// archive. A safe copy in this context means that when accessing
// the copy one does not need to worry about atomics or the string
// interning.
// It is important that ArchiveInterned.ReleaseInterned() gets called
// before it goes out of scope to return its memory back to the pools.
func (a *ArchiveInterned) CloneInterned() *ArchiveInterned {
	safeArchive := archiveInternedPool.Get().(*ArchiveInterned)
	safeArchive.SchemaId = a.SchemaId
	safeArchive.AggId = a.AggId
	safeArchive.IrId = a.IrId
	safeArchive.LastSave = atomic.LoadUint32(&a.LastSave)
	safeArchive.MetricDefinitionInterned = a.MetricDefinitionInterned.CloneInterned()

	archiveInternedCount.AddUint32(1)
	return safeArchive
}

func (s *ArchiveInterned) ReleaseInterned() {
	s.MetricDefinitionInterned.ReleaseInterned()
	archiveInternedPool.Put(s)
	archiveInternedCount.DecUint32(1)
}
