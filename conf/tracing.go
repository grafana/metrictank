package conf

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
)

// GetTracer returns a jaeger tracer
// any tags specified will be added as process/tracer-level tags
func GetTracer(enabled bool, addr string, tags map[string]string, sampleRatio float64) (opentracing.Tracer, io.Closer, error) {
	// Sample configuration for testing. Use sampleRatio >= 1.0 to sample every trace
	// and enable LogSpan to log every span via configured Logger.

	// Must be between 0 and 1 (inclusive)
	if sampleRatio < 0.0 {
		sampleRatio = 0.0
	} else if sampleRatio > 1.0 {
		sampleRatio = 1.0
	}

	cfg := jaegercfg.Configuration{
		Disabled: !enabled,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeProbabilistic,
			Param: sampleRatio,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans:           false,
			LocalAgentHostPort: addr,
		},
	}

	jLogger := jaegerlog.StdLogger

	options := []jaegercfg.Option{
		jaegercfg.Logger(jLogger),
	}
	for k, v := range tags {
		options = append(options, jaegercfg.Tag(k, v))
	}

	tracer, closer, err := cfg.New(
		"metrictank",
		options...,
	)
	if err != nil {
		return nil, nil, err
	}
	opentracing.InitGlobalTracer(tracer)
	return tracer, closer, nil
}
