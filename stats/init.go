// Package stats provides functionality for instrumenting metrics and reporting them
//
// The metrics can be user specified, or sourced from the runtime (reporters)
// To use this package correctly, you must instantiate exactly 1 output.
// If you use 0 outputs, certain metrics type will accumulate data unboundedly
// (e.g. histograms and meters) resulting in unreasonable memory usage.
// (though you can ignore this for shortlived processes, unit tests, etc)
// If you use >1 outputs, then each will only see a partial view of the stats.
// Currently supported outputs are DevNull and Graphite
package stats

var registry *Registry

func init() {
	registry = NewRegistry()
}

func Clear() {
	registry.Clear()
}
