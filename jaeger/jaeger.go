package jaeger

import (
	"io"
	"log"
	"strings"
	"time"

	"flag"

	"github.com/grafana/globalconf"
	opentracing "github.com/opentracing/opentracing-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
)

var (
	Enabled                bool
	addTagsRaw             string
	addTagsParsed          map[string]string
	samplerType            string
	samplerParam           float64
	samplerManagerAddr     string
	samplerMaxOperations   int
	samplerRefreshInterval time.Duration
	reporterMaxQueueSize   int
	reporterFlushInterval  time.Duration
	reporterLogSpans       bool
	collectorAddr          string
	collectorUser          string
	collectorPassword      string
	agentAddr              string
)

func ConfigSetup() {
	jaegerConf := flag.NewFlagSet("jaeger", flag.ExitOnError)
	jaegerConf.BoolVar(&Enabled, "enabled", false, "Whether the tracer is enabled or not")
	jaegerConf.StringVar(&addTagsRaw, "add-tags", "", "A comma separated list of name=value tracer level tags, which get added to all reported spans")
	jaegerConf.StringVar(&samplerType, "sampler-type", "const", "the type of the sampler: const, probabilistic, rateLimiting, or remote")
	jaegerConf.Float64Var(&samplerParam, "sampler-param", 1, "the sampler parameter (number)")
	jaegerConf.StringVar(&samplerManagerAddr, "sampler-manager-addr", "http://jaeger:5778/sampling", "The HTTP endpoint when using the remote sampler")
	jaegerConf.IntVar(&samplerMaxOperations, "sampler-max-operations", 0, "The maximum number of operations that the sampler will keep track of")
	jaegerConf.DurationVar(&samplerRefreshInterval, "sampler-refresh-interval", time.Second*10, "How often the remotely controlled sampler will poll jaeger-agent for the appropriate sampling strategy")
	jaegerConf.IntVar(&reporterMaxQueueSize, "reporter-max-queue-size", 0, "The reporter's maximum queue size")
	jaegerConf.DurationVar(&reporterFlushInterval, "reporter-flush-interval", time.Second*10, "The reporter's flush interval")
	jaegerConf.BoolVar(&reporterLogSpans, "reporter-log-spans", false, "Whether the reporter should also log the spans")
	jaegerConf.StringVar(&collectorAddr, "collector-addr", "", "HTTP endpoint for sending spans directly to a collector, i.e. http://jaeger-collector:14268/api/traces")
	jaegerConf.StringVar(&collectorUser, "collector-user", "", "Username to send as part of 'Basic' authentication to the collector endpoint")
	jaegerConf.StringVar(&collectorPassword, "collector-password", "", "Password to send as part of 'Basic' authentication to the collector endpoint")
	jaegerConf.StringVar(&agentAddr, "agent-addr", "Localhost:6831", "UDP address of the agent to send spans to. (only used if collector-endpoint is empty)")
	globalconf.Register("jaeger", jaegerConf, flag.ExitOnError)
}

func ConfigProcess() {
	addTagsParsed = make(map[string]string)
	addTagsRaw = strings.TrimSpace(addTagsRaw)
	if len(addTagsRaw) == 0 {
		return
	}
	tagSpecs := strings.Split(addTagsRaw, ",")
	for _, tagSpec := range tagSpecs {
		split := strings.Split(tagSpec, "=")
		if len(split) != 2 {
			log.Fatalf("cannot parse add-tags value %q", tagSpec)
		}
		addTagsParsed[split[0]] = split[1]
	}
}

// Get() returns a jaeger tracer
func Get() (opentracing.Tracer, io.Closer, error) {
	cfg := jaegercfg.Configuration{
		Disabled: !Enabled,
		Sampler: &jaegercfg.SamplerConfig{
			Type:                    samplerType,
			Param:                   samplerParam,
			SamplingServerURL:       samplerManagerAddr,
			MaxOperations:           samplerMaxOperations,
			SamplingRefreshInterval: samplerRefreshInterval,
		},
		Reporter: &jaegercfg.ReporterConfig{
			QueueSize:           reporterMaxQueueSize,
			BufferFlushInterval: reporterFlushInterval,
			LogSpans:            reporterLogSpans,
			LocalAgentHostPort:  agentAddr,
			CollectorEndpoint:   collectorAddr,
			User:                collectorUser,
			Password:            collectorPassword,
		},
	}

	jLogger := jaegerlog.StdLogger
	options := []jaegercfg.Option{
		jaegercfg.Logger(jLogger),
	}
	for k, v := range addTagsParsed {
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
