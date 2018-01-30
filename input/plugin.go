package input

type Plugin interface {
	Name() string
	// Start starts the plugin.
	// The plugin closes the fatal chan should any non-recoverable error occur after Start has returned.
	// if Start returns an error, or the fatal chan is closed by the plugin,
	// the caller (e.g. main process) should shut down all its resources and exit.
	// Note that upon fatal close, metrictank will call Stop() on all plugins, also the one that triggered it.
	Start(handler Handler, fatal chan struct{}) error
	MaintainPriority()
	Stop() // Should block until shutdown is complete.
}
