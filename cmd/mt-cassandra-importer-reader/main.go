package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/mdata/importer"
	"github.com/grafana/metrictank/schema"
	"github.com/opentracing/opentracing-go"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const Month_sec = 60 * 60 * 24 * 28

type Config struct {
	WriteEndpoint         string
	EndpointAuth          string
	Threads               int
	WriteUnfinishedChunks bool
	ImportFrom            int
	ImportUntil           int
	Partitions            string
	Host                  string
	Keyspace              string
	Table                 string
	DryRun                bool
}

func NewConfig() *Config {
	return &Config{
		WriteEndpoint: "http://127.0.0.1:8080/metrics/import",
		EndpointAuth:  "user:password",
		Threads:       10,
		ImportFrom:    0,
		ImportUntil:   int(time.Now().Unix()),
		Partitions:    "0",
		Host:          "localhost:9042",
		Keyspace:      "metrictank",
		Table:         "metric_1024",
		DryRun:        false,
	}
}

func (c *Config) Validate() error {
	return nil
}

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	var verbose bool
	cfg := NewConfig()
	flag.BoolVar(&verbose, "verbose", false, "enable debug logging")
	flag.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "just log what would be imported but dont import")
	flag.StringVar(&cfg.WriteEndpoint, "write-endpoint", cfg.WriteEndpoint, "The http endpoint to send the data to")
	flag.StringVar(&cfg.EndpointAuth, "endpoint-auth", cfg.EndpointAuth, "The credentials used to authenticate in the format \"user:password\"")
	flag.IntVar(&cfg.Threads, "threads", cfg.Threads, "number of concurrent threads to use")
	flag.IntVar(&cfg.ImportFrom, "import-from", cfg.ImportFrom, "epoch timestamp to import from")
	flag.IntVar(&cfg.ImportUntil, "import-until", cfg.ImportUntil, "epoch timestamp to import until")
	flag.StringVar(&cfg.Partitions, "partitions", cfg.Partitions, "comma separated list of partitions to process")
	flag.StringVar(&cfg.Host, "host", cfg.Host, "Address of cassandra node")
	flag.StringVar(&cfg.Keyspace, "keyspace", cfg.Keyspace, "cassandra keyspace to read from")
	flag.StringVar(&cfg.Table, "table", cfg.Table, "cassandra table to read from")
	flag.Parse()

	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	g, ctx := errgroup.WithContext(context.Background())
	ch := make(chan *schema.MetricDefinition, 1000)

	cluster.Init("mt-cassandra-importer", "0.1", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)

	cluster := gocql.NewCluster(cfg.Host)
	cluster.Consistency = gocql.One
	cluster.Timeout = time.Minute
	cluster.NumConns = cfg.Threads * 2
	cluster.ProtoVersion = 4
	cluster.Keyspace = cfg.Keyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 5}
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to connect to cassandra. err=%s", err)
	}
	defer session.Close()

	// start threads for reading from the index
	g.Go(func() error {
		log.Infof("launching index reader threads")
		return launchIndexHandler(ctx, cfg, session, ch)
	})
	// start threads for reading chunks and writing to remote writeEndpoint
	g.Go(func() error {
		log.Infof("launching chunk threads")
		return launchChunkHandler(ctx, cfg, session, ch)
	})

	g.Wait()
	log.Infof("processing complete.")
}

func launchIndexHandler(ctx context.Context, cfg *Config, session *gocql.Session, ch chan *schema.MetricDefinition) error {
	// close the MetricDef chan when we are done, so that the chunk handler knows not to expect any
	// more defs to process.
	defer close(ch)
	query := "SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from metric_idx where partition = ?"

	// parse partitions into slice of ints
	partitionsList := strings.Split(cfg.Partitions, ",")
	partitions := make([]int, len(partitionsList))

	for i, p := range partitionsList {
		pID, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Errorf("failed to parse partitions. err=%s", err)
			return err
		}
		partitions[i] = pID
	}

	g, groupCtx := errgroup.WithContext(ctx)
	for _, part := range partitions {
		p := part
		g.Go(func() error {
			log.Infof("fetching items in the index for partition %d", p)
			iter := session.Query(query, p).WithContext(groupCtx).Iter()
			var id, name, unit, mtype string
			var orgId, interval int
			var partition int32
			var lastupdate int64
			var tags []string
			for iter.Scan(&id, &orgId, &partition, &name, &interval, &unit, &mtype, &tags, &lastupdate) {
				mkey, err := schema.MKeyFromString(id)
				if err != nil {
					log.Errorf("cassandra-idx: load() could not parse ID %q: %s -> skipping", id, err)
					continue
				}
				if interval != 300 {
					continue
				}

				mdef := &schema.MetricDefinition{
					Id:         mkey,
					OrgId:      uint32(orgId),
					Partition:  partition,
					Name:       name,
					Interval:   interval,
					Unit:       unit,
					Mtype:      mtype,
					Tags:       tags,
					LastUpdate: lastupdate,
				}
				select {
				case <-groupCtx.Done():
					iter.Close()
					return groupCtx.Err()
				case ch <- mdef:
				}
			}
			if err := iter.Close(); err != nil {
				log.Errorf("Could not close iterator: %s", err.Error())
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func launchChunkHandler(ctx context.Context, cfg *Config, session *gocql.Session, ch chan *schema.MetricDefinition) error {
	flush := func(a *importer.ArchiveRequest) error {
		if cfg.DryRun {
			// just print what data we would write
			mkey, _ := schema.MKeyFromString(a.MetricData.Id)
			for _, req := range a.ChunkWriteRequests {
				key := schema.AMKey{MKey: mkey, Archive: req.Archive}
				log.Infof("KEY:%s T0:%d TTL:%d bytes:%d", key.String(), req.T0, req.TTL, len(req.Data))
			}
			return nil
		}
		success := false
		attempts := 0
		for !success {
			b, err := a.MarshalCompressed()
			if err != nil {
				log.Errorf("Failed to encode metric: %q", err.Error())
				return err
			}
			size := b.Len()

			req, err := http.NewRequest("POST", cfg.WriteEndpoint, io.Reader(b))
			if err != nil {
				log.Errorf("Cannot construct request to http endpoint %q: %q", cfg.WriteEndpoint, err.Error())
				return err
			}

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", "gzip")

			// set x-tsdb-org, will only be honored if an admin api key is used.
			req.Header.Set("X-Tsdb-Org", strconv.FormatInt(int64(a.MetricData.OrgId), 10))

			if len(cfg.EndpointAuth) > 0 {
				req.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(cfg.EndpointAuth)))
			}

			pre := time.Now()
			resp, err := http.DefaultClient.Do(req)
			passed := time.Now().Sub(pre).Seconds()
			if err != nil || resp.StatusCode >= 300 {
				if err != nil {
					log.Warningf("Error posting %s (%d bytes), to endpoint %q (attempt %d/%fs, retrying): %s", a.MetricData.Id, size, cfg.WriteEndpoint, attempts, passed, err.Error())
					attempts++
					continue
				} else {
					log.Warningf("Error posting %s (%d bytes) to endpoint %q status %d (attempt %d/%fs, retrying)", a.MetricData.Id, size, cfg.WriteEndpoint, resp.StatusCode, attempts, passed)
				}
				attempts++
			} else {
				log.Debugf("Posted %s (%d bytes) to endpoint %q in %f seconds", a.MetricData.Id, size, cfg.WriteEndpoint, passed)
				success = true
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil
	}

	g, groupCtx := errgroup.WithContext(ctx)
	for i := 0; i < cfg.Threads; i++ {
		g.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case def, ok := <-ch:
					if !ok {
						return nil
					}

					// fetch all of the chunks for this def, and send them to the remoteWriteEndpoint
					err := processDef(groupCtx, def, cfg.ImportFrom, cfg.ImportUntil, session, cfg.Table, flush)
					if err != nil {
						log.Errorf("failed to process chunks for def with id=%s. err=%s", def.Id.String(), err)
						return err
					}
				}
			}
		})
	}
	return g.Wait()
}

func processDef(ctx context.Context, def *schema.MetricDefinition, from, until int, session *gocql.Session, table string, flush func(a *importer.ArchiveRequest) error) error {
	query := fmt.Sprintf("SELECT ts, data from %s WHERE key=? AND ts >= ? AND ts < ? ORDER BY ts ASC", table)
	startMonth := from / Month_sec
	endMonth := until / Month_sec
	md := schema.MetricData{
		Id:       def.Id.String(),
		OrgId:    int(def.OrgId),
		Name:     def.Name,
		Value:    0,
		Interval: def.Interval,
		Unit:     def.Unit,
		Time:     def.LastUpdate,
		Mtype:    def.Mtype,
		Tags:     def.Tags,
	}
	ar := &importer.ArchiveRequest{
		MetricData:         md,
		ChunkWriteRequests: make([]importer.ChunkWriteRequest, 0),
	}
	var data []byte
	var ts int
	store := new(AggStore)
	aggregators := newAggregators(def, store, from, until)
	for i := startMonth; i <= endMonth; i++ {
		rowKey := fmt.Sprintf("%s_%d", def.Id.String(), i)
		log.Infof("fetching row with Key=%s", rowKey)
		iter := session.Query(query, rowKey, from, until).WithContext(ctx).Iter()
		for iter.Scan(&ts, &data) {
			ar.ChunkWriteRequests = append(ar.ChunkWriteRequests, importer.NewChunkWriteRequest(
				0,
				dur.MustParseNDuration("1y TTL", "1y"), //TODO: this needs to be updated to be based on def.Interval and the retention policy.
				uint32(ts),
				data,
				time.Now(),
			))
			err := aggregators.aggregate(data, ts)
			if err != nil {
				log.Errorf("failed to generate rollups for row with key=%s, err=%v", rowKey, err)
				iter.Close()
				return err
			}
			ar.ChunkWriteRequests = append(ar.ChunkWriteRequests, store.Get()...)
		}
		if err := iter.Close(); err != nil {
			log.Errorf("Could not close iterator: %s", err.Error())
			return err
		}
	}

	ar.ChunkWriteRequests = append(ar.ChunkWriteRequests, store.Get()...)
	return flush(ar)
}

// alignForward aligns ts to the next timestamp that divides by the interval, except if it is already aligned
func alignForward(ts, interval uint32) uint32 {
	remain := ts % interval
	if remain == 0 {
		return ts
	}
	return ts + interval - remain
}

// alignBackward aligns the ts to the previous ts that divides by the interval, even if it is already aligned
func alignBackward(ts uint32, interval uint32) uint32 {
	return ts - ((ts-1)%interval + 1)
}

type aggregator struct {
	Aggregator *mdata.Aggregator
	dropAfter  uint32
	dropBefore uint32
	retention  conf.Retention
}

type aggregators struct {
	aggs  []aggregator
	md    *schema.MetricDefinition
	from  int
	until int
	store *AggStore
}

func newAggregators(md *schema.MetricDefinition, store *AggStore, from, until int) *aggregators {
	retentions := []conf.Retention{ //TODO: these should be generated based on the storage-schemas and MetricDefinition
		{
			SecondsPerPoint: 1200,
			NumberOfPoints:  int(dur.MustParseNDuration("5y TTL", "5y")) / 1200,
			ChunkSpan:       60 * 60 * 6,
			NumChunks:       1,
			Ready:           0,
		},
	}

	// TODO: This assumes that we only want avg (which is stored as sum and count aggregates)
	agg := conf.NewAggregations().DefaultAggregation

	aggs := make([]aggregator, 0, len(retentions))
	for _, ret := range retentions {
		aggs = append(aggs, aggregator{
			Aggregator: mdata.NewAggregator(store, store, schema.AMKey{MKey: md.Id, Archive: 0}, ret, agg, false, 0),
			retention:  ret,
			// for the last raw chunk processed, we want to ignore all points in the last rollup interval, as these
			// points will be used as the first point of the next chunk
			dropAfter: (alignBackward(uint32(until), ret.ChunkSpan) + ret.ChunkSpan - uint32(ret.SecondsPerPoint)),
			// for the first chunk processed, we want to skip all but the last rollup interval.  This
			// is because the first point of a rollup chunk comes from the last points of a raw chunk.
			dropBefore: (alignForward(uint32(from), ret.ChunkSpan) + ret.ChunkSpan - uint32(ret.SecondsPerPoint)),
		})
	}
	return &aggregators{
		aggs:  aggs,
		md:    md,
		from:  from,
		until: until,
		store: store,
	}
}

// Aggregate takes raw chunks and creates 300s:1y and 1200s:5y rollup chunks
func (a *aggregators) aggregate(data []byte, ts int) error {
	iterGen, err := chunk.NewIterGen(uint32(ts), uint32(a.md.Interval), data)
	if err != nil {
		return err
	}

	iter, err := iterGen.Get()
	if err != nil {
		return err
	}

	for iter.Next() {
		t, val := iter.Values()
		for _, agg := range a.aggs {
			if t <= agg.dropBefore || t > agg.dropAfter {
				//fmt.Printf("Skipped adding rollup with interval %d: %d <= %d < %d\n", agg.retention.SecondsPerPoint, agg.dropBefore, t, agg.dropAfter)
				continue
			}
			//fmt.Printf("added %d to rollup with interval %d\n", t, agg.retention.SecondsPerPoint)
			agg.Aggregator.Add(t, val)
		}
	}

	return nil
}

type AggStore struct {
	CWR []importer.ChunkWriteRequest
}

func (a *AggStore) Add(cwr *mdata.ChunkWriteRequest) {
	log.Debugf("new chunk for %s created", cwr.Key.String())
	a.CWR = append(a.CWR, importer.ChunkWriteRequest{
		ChunkWriteRequestPayload: cwr.ChunkWriteRequestPayload,
		Archive:                  cwr.Key.Archive,
	})
}

func (a *AggStore) Search(ctx context.Context, key schema.AMKey, ttl, from, to uint32) ([]chunk.IterGen, error) {
	return nil, nil
}

func (a *AggStore) Get() []importer.ChunkWriteRequest {
	requests := make([]importer.ChunkWriteRequest, len(a.CWR))
	copy(requests, a.CWR)
	a.CWR = a.CWR[:0]
	return requests
}

func (a *AggStore) Stop() {}

func (a *AggStore) SetTracer(t opentracing.Tracer) {}

func (a *AggStore) AddIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen) {}
