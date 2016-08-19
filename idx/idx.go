// The idx package provides a metadata index for metrics

package idx

import (
	"errors"

	"github.com/raintank/met"
	"gopkg.in/raintank/schema.v1"
)

var (
	DefNotFound = errors.New("MetricDef not found")
)

type Node struct {
	Path string
	Leaf bool
	Defs []schema.MetricDefinition
}

type MetricIndex interface {
	Init(met.Backend) error
	Stop()
	Add(*schema.MetricData)
	Get(string) (schema.MetricDefinition, error)
	Delete(int, string) error
	Find(int, string) ([]Node, error)
	List(int) []schema.MetricDefinition
}
