package conf

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// GetTracer returns a jaeger tracer
// Jaeger is configured through environment variables
func GetTracer() (opentracing.Tracer, io.Closer, error) {
	cfg, err := jaegercfg.FromEnv()
	if err != nil {
		return nil, nil, err
	}
	cfg.ServiceName = "metrictank"

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		return nil, nil, err
	}

	opentracing.InitGlobalTracer(tracer)
	return tracer, closer, nil
}
