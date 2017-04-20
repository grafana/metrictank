package input

type Plugin interface {
	Name() string
	Start(handler Handler)
	MaintainPriority()
	Stop() // Should block until shutdown is complete.
}
