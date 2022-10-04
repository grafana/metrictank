package jaeger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"flag"

	"github.com/grafana/globalconf"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
)

var (
	Enabled                bool
	addTagsRaw             string
	addTagsParsed          []opentracing.Tag
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
	var err error
	addTagsParsed, err = parseTags(addTagsRaw)
	if err != nil {
		log.Fatalf("jaeger: Config validation error. %s", err)
	}
}

// parseTags parses the given string into a slice of opentracing.Tag.
// the string must be a comma separated list of key=value pairs, where value
// can be specified as ${key:default}, where `key` is an environment
// variable and `default` is the value to use in case the env var is not set
func parseTags(input string) ([]opentracing.Tag, error) {
	pairs := strings.Split(input, ",")
	var tags []opentracing.Tag
	for _, pair := range pairs {
		if pair == "" {
			continue
		}

		if !strings.Contains(pair, "=") {
			return nil, fmt.Errorf("invalid tag specifier %q", pair)
		}
		kv := strings.SplitN(pair, "=", 2)
		key, val := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if len(key) == 0 || len(val) == 0 {
			return nil, fmt.Errorf("invalid tag specifier %q", pair)
		}

		if strings.HasPrefix(val, "${") && strings.HasSuffix(val, "}") {
			spec := strings.SplitN(val[2:len(val)-1], ":", 2)
			envVar := spec[0]
			var envDefault string
			if len(spec) == 2 {
				envDefault = spec[1]
			}
			val = os.Getenv(envVar)
			if val == "" && envDefault != "" {
				val = envDefault
			}
		}

		tags = append(tags, opentracing.Tag{Key: key, Value: val})
	}

	return tags, nil
}

// Get() returns a jaeger tracer
func Get() (opentracing.Tracer, io.Closer, error) {
	cfg := jaegercfg.Configuration{
		Disabled: !Enabled,
		Tags:     addTagsParsed,
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
