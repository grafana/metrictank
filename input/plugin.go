package input

type Plugin interface {
	Name() string
	Start(handler Handler)
	Stop() // Should block until shutdown is complete.
}
