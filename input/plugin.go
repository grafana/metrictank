package input

type Plugin interface {
	Name() string
	// Start starts the plugin. It's up to the plugin to close the fatal chan should any non-recoverable error occur
	// the fatal chan signals to the caller (e.g. main process) that it should shut down all its resources and exit.
	// Note that upon fatal close, metrictank will call Stop() on all plugins, also the one that triggered it.
	Start(handler Handler, fatal chan struct{})
	MaintainPriority()
	Stop() // Should block until shutdown is complete.
}
