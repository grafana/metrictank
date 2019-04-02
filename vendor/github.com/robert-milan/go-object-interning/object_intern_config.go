package goi

type Compression uint8

// Types of compression
const (
	None Compression = iota
	Shoco
	ShocoDict
)

// Config provides a configuration with default settings
var Config = NewConfig()

// ObjectInternConfig holds a configuration to use when creating a new ObjectIntern.
// Currently, Index and MaxIndexSize don't do anything.
type ObjectInternConfig struct {
	Compression  Compression
	Index        bool
	MaxIndexSize uint32
}

// NewConfig returns a new configuration with default settings
//
// Compression: 	None,
// Index:			true,
// MaxCacheSize: 	157286400,
func NewConfig() ObjectInternConfig {
	return ObjectInternConfig{
		Compression:  None,
		Index:        true,
		MaxIndexSize: 157286400, // 150 MiB
	}
}
