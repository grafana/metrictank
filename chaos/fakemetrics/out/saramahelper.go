package out

import (
	"github.com/Shopify/sarama"
	"github.com/raintank/worldping-api/pkg/log"
)

func GetCompression(codec string) sarama.CompressionCodec {
	switch codec {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	default:
		log.Fatal(5, "unknown compression codec %q", codec)
		return 0 // make go compiler happy, needs a return *roll eyes*
	}
}
