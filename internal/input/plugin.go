package input

import "context"

type Plugin interface {
	Name() string
	// Start starts the plugin.
	// The plugin calls cancel should any non-recoverable error occur after Start has returned.
	// if Start returns an error, or cancel is called by the plugin,
	// the caller (e.g. main process) should shut down all its resources and exit.
	// Note that upon fatal close, metrictank will call Stop() on all plugins, also the one that triggered it.
	Start(handler Handler, cancel context.CancelFunc) error
	MaintainPriority()
	ExplainPriority() interface{}
	Stop() // Should block until shutdown is complete.
}
