package goi

// Types of compression
const (
	NOCPRSN = iota
	SHOCO
	SHOCODICT
)

// Config provides a configuration with default settings
var Config = NewConfig()

// ObjectInternConfig holds a configuration to use when creating a new ObjectIntern.
// Currently, Index and MaxIndexSize don't do anything.
type ObjectInternConfig struct {
	CompressionType uint8
	Index           bool
	MaxIndexSize    uint32
}

// NewConfig returns a new configuration with default settings
//
// CompressType: SHOCO,
// Cache: true,
// MasCacheSize: 157286400,
func NewConfig() *ObjectInternConfig {
	return &ObjectInternConfig{
		CompressionType: NOCPRSN,
		Index:           true,
		MaxIndexSize:    157286400, // 150 MB
	}
}
