package idx

import (
	"testing"

	"github.com/raintank/schema"
)

func BenchmarkCreatingSafeArchives(b *testing.B) {
	mkey, err := schema.MKeyFromString("1.12345678901234567890123456789012")
	if err != nil {
		b.Fatalf("Failed to get amkey: %s", err)
	}
	arch := &ArchiveInterned{
		SchemaId: 1,
		AggId:    2,
		IrId:     3,
		LastSave: 4,
		MetricDefinition: &MetricDefinition{
			Id:         mkey,
			OrgId:      1,
			Interval:   10,
			LastUpdate: 5,
			Partition:  6,
		},
	}

	err = arch.MetricDefinition.SetMetricName("a.b.c")
	if err != nil {
		b.Fatalf("Failed to set metric name: %s", err)
	}

	arch.MetricDefinition.SetUnit("s")
	arch.MetricDefinition.SetMType("gauge")
	arch.MetricDefinition.SetTags([]string{"a=b", "c=d"})

	b.ReportAllocs()
	b.ResetTimer()

	for i := uint32(0); i < uint32(b.N); i++ {
		copy := NewSafeArchive(arch)
		copy.ReleaseSafeArchive()
	}
}
