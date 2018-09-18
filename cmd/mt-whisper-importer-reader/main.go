package main

import (
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/mdata/chunk/archive"
	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

var (
	httpEndpoint = flag.String(
		"http-endpoint",
		"http://127.0.0.1:8080/chunks",
		"The http endpoint to send the data to",
	)
	namePrefix = flag.String(
		"name-prefix",
		"",
		"Prefix to prepend before every metric name, should include the '.' if necessary",
	)
	threads = flag.Int(
		"threads",
		10,
		"Number of workers threads to process and convert .wsp files",
	)
	writeUnfinishedChunks = flag.Bool(
		"write-unfinished-chunks",
		false,
		"Defines if chunks that have not completed their chunk span should be written",
	)
	orgId = flag.Int(
		"orgid",
		1,
		"Organization ID the data belongs to ",
	)
	insecureSSL = flag.Bool(
		"insecure-ssl",
		false,
		"Disables ssl certificate verification",
	)
	whisperDirectory = flag.String(
		"whisper-directory",
		"/opt/graphite/storage/whisper",
		"The directory that contains the whisper file structure",
	)
	httpAuth = flag.String(
		"http-auth",
		"",
		"The credentials used to authenticate in the format \"user:password\"",
	)
	dstSchemas = flag.String(
		"dst-schemas",
		"",
		"The filename of the output schemas definition file",
	)
	nameFilterPattern = flag.String(
		"name-filter",
		"",
		"A regex pattern to be applied to all metric names, only matching ones will be imported",
	)
	importUpTo = flag.Uint(
		"import-up-to",
		math.MaxUint32,
		"Only import up to the specified timestamp",
	)
	importAfter = flag.Uint(
		"import-after",
		0,
		"Only import after the specified timestamp",
	)
	positionFile = flag.String(
		"position-file",
		"",
		"file to store position and load position from",
	)
	verbose = flag.Bool(
		"verbose",
		false,
		"More detailed logging",
	)
	schemas        conf.Schemas
	nameFilter     *regexp.Regexp
	processedCount uint32
	skippedCount   uint32
)

func main() {
	var err error
	flag.Parse()

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.QuoteEmptyFields = true
	log.SetFormatter(formatter)
	if *verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	nameFilter = regexp.MustCompile(*nameFilterPattern)
	schemas, err = conf.ReadSchemas(*dstSchemas)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("error when parsing schemas file")
	}

	var pos *posTracker
	if len(*positionFile) > 0 {
		pos, err = NewPositionTracker(*positionFile)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Fatal("error instantiating position tracker")
		}
		defer pos.Close()
	}

	fileChan := make(chan string)

	wg := &sync.WaitGroup{}
	wg.Add(*threads)
	for i := 0; i < *threads; i++ {
		go processFromChan(pos, fileChan, wg)
	}

	getFileListIntoChan(pos, fileChan)
	wg.Wait()
}

func processFromChan(pos *posTracker, files chan string, wg *sync.WaitGroup) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: *insecureSSL},
	}
	client := &http.Client{Transport: tr}

	for file := range files {
		fd, err := os.Open(file)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"file":  file,
			}).Error("failed to open whisper file")
			continue
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"file":  file,
			}).Error("failed to open whisper file")
			continue
		}

		name := getMetricName(file)
		log.Debugf("Processing file %s (%s)", file, name)
		met, err := getMetric(w, file, name)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("failed to get metric")
			continue
		}

		success := false
		attempts := 0
		for !success {
			b, err := met.MarshalCompressed()
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("failed to get metric")
				continue
			}
			size := b.Len()

			req, err := http.NewRequest("POST", *httpEndpoint, io.Reader(b))
			if err != nil {
				log.WithFields(log.Fields{
					"http.endpoint": *httpEndpoint,
					"error":         err.Error(),
				}).Fatal("cannot construct request to http endpoint")
			}

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", "gzip")

			if len(*httpAuth) > 0 {
				req.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(*httpAuth)))
			}

			pre := time.Now()
			resp, err := client.Do(req)
			passed := time.Now().Sub(pre).Seconds()
			if err != nil || resp.StatusCode >= 300 {
				if err != nil {
					log.WithFields(log.Fields{
						"name":                 name,
						"size.bytes":           size,
						"http.endpoint":        *httpEndpoint,
						"attempts":             attempts,
						"time.elapsed.seconds": passed,
						"error":                err.Error(),
					}).Warn("error posting to endpoint, retrying")
					attempts++
					continue
				} else {
					log.WithFields(log.Fields{
						"name":                 name,
						"size.bytes":           size,
						"http.endpoint":        *httpEndpoint,
						"resp.status.code":     resp.StatusCode,
						"attempts":             attempts,
						"time.elapsed.seconds": passed,
						"error":                err.Error(),
					}).Warn("error posting to endpoint, retrying")
				}
				attempts++
			} else {
				log.WithFields(log.Fields{
					"name":                 name,
					"size.bytes":           size,
					"http.endpoint":        *httpEndpoint,
					"time.elapsed.seconds": passed,
				}).Debug("posted to endpoint")
				success = true
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}

		if pos != nil {
			pos.Done(file)
		}
		processed := atomic.AddUint32(&processedCount, 1)
		if processed%100 == 0 {
			skipped := atomic.LoadUint32(&skippedCount)
			log.WithFields(log.Fields{
				"files.processed": processed,
				"files.skipped":   skipped,
			}).Info("processed files")
		}
	}
	wg.Done()
}

// generate the metric name based on the file name and given prefix
func getMetricName(file string) string {
	// remove all leading '/' from file name
	file = strings.TrimPrefix(file, *whisperDirectory)
	for file[0] == '/' {
		file = file[1:]
	}

	return *namePrefix + strings.Replace(strings.TrimSuffix(file, ".wsp"), "/", ".", -1)
}

// pointSorter sorts points by timestamp
type pointSorter []whisper.Point

func (a pointSorter) Len() int           { return len(a) }
func (a pointSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a pointSorter) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

// the whisper archives are organized like a ringbuffer. since we need to
// insert the points into the chunks in order we first need to sort them
func sortPoints(points pointSorter) pointSorter {
	sort.Sort(points)
	return points
}

func shortAggMethodString(aggMethod whisper.AggregationMethod) (string, error) {
	switch aggMethod {
	case whisper.AggregationAverage:
		return "avg", nil
	case whisper.AggregationSum:
		return "sum", nil
	case whisper.AggregationMin:
		return "min", nil
	case whisper.AggregationMax:
		return "max", nil
	case whisper.AggregationLast:
		return "lst", nil
	default:
		return "", fmt.Errorf("Unknown aggregation method %d", aggMethod)
	}
}

func getMetric(w *whisper.Whisper, file, name string) (archive.Metric, error) {
	res := archive.Metric{
		AggregationMethod: uint32(w.Header.Metadata.AggregationMethod),
	}
	if len(w.Header.Archives) == 0 {
		return res, fmt.Errorf("Whisper file contains no archives: %q", file)
	}

	method, err := shortAggMethodString(w.Header.Metadata.AggregationMethod)
	if err != nil {
		return res, err
	}

	md := schema.MetricData{
		Name:     name,
		Interval: int(w.Header.Archives[0].SecondsPerPoint),
		Value:    0,
		Unit:     "unknown",
		Time:     0,
		Mtype:    "gauge",
		Tags:     []string{},
		OrgId:    *orgId,
	}
	md.SetId()
	_, schem := schemas.Match(md.Name, int(w.Header.Archives[0].SecondsPerPoint))

	points := make(map[int][]whisper.Point)
	for i := range w.Header.Archives {
		p, err := w.DumpArchive(i)
		if err != nil {
			return res, fmt.Errorf("Failed to dump archive %d from whisper file %s", i, file)
		}
		points[i] = p
	}

	conversion := newConversion(w.Header.Archives, points, method)
	for retIdx, retention := range schem.Retentions {
		convertedPoints := conversion.getPoints(retIdx, uint32(retention.SecondsPerPoint), uint32(retention.NumberOfPoints))
		for m, p := range convertedPoints {
			if len(p) == 0 {
				continue
			}
			mkey, err := schema.MKeyFromString(md.Id)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Panic("failed to retrieve mkey from id")
			}
			rowKey := getRowKey(retIdx, mkey, m, retention.SecondsPerPoint)
			encodedChunks := encodedChunksFromPoints(p, uint32(retention.SecondsPerPoint), retention.ChunkSpan)
			log.WithFields(log.Fields{
				"archive":    retIdx,
				"method":     m,
				"num.points": len(p),
				"num.chunks": len(encodedChunks),
				"span":       retention.ChunkSpan,
			}).Info("archive operation")
			res.Archives = append(res.Archives, archive.Archive{
				SecondsPerPoint: uint32(retention.SecondsPerPoint),
				Points:          uint32(retention.NumberOfPoints),
				Chunks:          encodedChunks,
				RowKey:          rowKey.String(),
			})
			if int64(p[len(p)-1].Timestamp) > md.Time {
				md.Time = int64(p[len(p)-1].Timestamp)
			}
		}
	}
	res.MetricData = md

	return res, nil
}

func getRowKey(retIdx int, mkey schema.MKey, meth string, secondsPerPoint int) schema.AMKey {
	if retIdx == 0 {
		return schema.AMKey{MKey: mkey}
	}
	m, _ := schema.MethodFromString(meth)
	return schema.AMKey{
		MKey:    mkey,
		Archive: schema.NewArchive(m, uint32(secondsPerPoint)),
	}
}

func encodedChunksFromPoints(points []whisper.Point, intervalIn, chunkSpan uint32) []chunk.IterGen {
	var point whisper.Point
	var t0, prevT0 uint32
	var c *chunk.Chunk
	var encodedChunks []chunk.IterGen

	for _, point = range points {
		// this shouldn't happen, but if it would we better catch it here because Metrictank wouldn't handle it well:
		// https://github.com/grafana/metrictank/blob/f1868cccfb92fc82cd853914af958f6d187c5f74/mdata/aggmetric.go#L378
		if point.Timestamp == 0 {
			continue
		}

		t0 = point.Timestamp - (point.Timestamp % chunkSpan)
		if prevT0 == 0 {
			c = chunk.New(t0)
			prevT0 = t0
		} else if prevT0 != t0 {
			c.Finish()

			encodedChunks = append(encodedChunks, *chunk.NewBareIterGen(c.Bytes(), c.T0, chunkSpan))

			c = chunk.New(t0)
			prevT0 = t0
		}

		err := c.Push(point.Timestamp, point.Value)
		if err != nil {
			log.WithFields(log.Fields{
				"t0.chunk": t0,
				"error":    err.Error(),
			}).Panic("failed to push value into chunk at t0")
		}
	}

	// if the last written point was also the last one of the current chunk,
	// or if writeUnfinishedChunks is on, we close the chunk and push it
	if point.Timestamp == t0+chunkSpan-intervalIn || *writeUnfinishedChunks {
		c.Finish()
		encodedChunks = append(encodedChunks, *chunk.NewBareIterGen(c.Bytes(), c.T0, chunkSpan))
	}

	return encodedChunks
}

// scan a directory and feed the list of whisper files relative to base into the given channel
func getFileListIntoChan(pos *posTracker, fileChan chan string) {
	filepath.Walk(
		*whisperDirectory,
		func(path string, info os.FileInfo, err error) error {
			if path == *whisperDirectory {
				return nil
			}
			name := getMetricName(path)
			if !nameFilter.Match([]byte(getMetricName(name))) {
				log.WithFields(log.Fields{
					"path": path,
					"file": name,
				}).Panic("skipping file")
				atomic.AddUint32(&skippedCount, 1)
				return nil
			}
			if len(path) < 4 || path[len(path)-4:] != ".wsp" {
				return nil
			}
			if pos != nil && pos.IsDone(path) {
				log.WithFields(log.Fields{
					"file": path,
				}).Panic("skipping file because it was listed as already done")
				return nil
			}

			fileChan <- path
			return nil
		},
	)

	close(fileChan)
}
