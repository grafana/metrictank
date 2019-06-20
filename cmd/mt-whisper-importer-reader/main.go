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
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/chunk"
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

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	var err error
	flag.Parse()
	if *verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	nameFilter = regexp.MustCompile(*nameFilterPattern)
	schemas, err = conf.ReadSchemas(*dstSchemas)
	if err != nil {
		panic(fmt.Sprintf("Error when parsing schemas file: %q", err))
	}

	var pos *posTracker
	if len(*positionFile) > 0 {
		pos, err = NewPositionTracker(*positionFile)
		if err != nil {
			log.Fatalf("Error instantiating position tracker: %s", err.Error())
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
			log.Errorf("Failed to open whisper file %q: %q\n", file, err.Error())
			continue
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			log.Errorf("Failed to open whisper file %q: %q\n", file, err.Error())
			continue
		}

		name := getMetricName(file)
		log.Debugf("Processing file %s (%s)", file, name)
		data, err := getMetric(w, file, name)
		if err != nil {
			log.Errorf("Failed to get metric: %q", err.Error())
			continue
		}

		if log.GetLevel() >= log.DebugLevel {
			details := fmt.Sprintf("Name: %s\nId: %s\nLastUpdate: %d\n", data.MetricData.Name, data.MetricData.Id, data.MetricData.Time)
			for _, cwr := range data.ChunkWriteRequests {
				details += fmt.Sprintf("Chunk write request T0: %d TTL: %d Size: %d\n", cwr.T0, cwr.TTL, len(cwr.Data))
			}
			log.Debugf("Sending archive request:\n%s", details)
		}

		success := false
		attempts := 0
		for !success {
			b, err := data.MarshalCompressed()
			if err != nil {
				log.Errorf("Failed to encode metric: %q", err.Error())
				continue
			}
			size := b.Len()

			req, err := http.NewRequest("POST", *httpEndpoint, io.Reader(b))
			if err != nil {
				log.Fatalf("Cannot construct request to http endpoint %q: %q", *httpEndpoint, err.Error())
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
					log.Warningf("Error posting %s (%d bytes), to endpoint %q (attempt %d/%fs, retrying): %s", name, size, *httpEndpoint, attempts, passed, err.Error())
					attempts++
					continue
				} else {
					log.Warningf("Error posting %s (%d bytes) to endpoint %q status %d (attempt %d/%fs, retrying)", name, size, *httpEndpoint, resp.StatusCode, attempts, passed)
				}
				attempts++
			} else {
				log.Debugf("Posted %s (%d bytes) to endpoint %q in %f seconds", name, size, *httpEndpoint, passed)
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
			log.Infof("Processed %d files, %d skipped", processed, skipped)
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

func convertWhisperMethod(whisperMethod whisper.AggregationMethod) (schema.Method, error) {
	switch whisperMethod {
	case whisper.AggregationAverage:
		return schema.Avg, nil
	case whisper.AggregationSum:
		return schema.Sum, nil
	case whisper.AggregationLast:
		return schema.Lst, nil
	case whisper.AggregationMax:
		return schema.Max, nil
	case whisper.AggregationMin:
		return schema.Min, nil
	default:
		return 0, fmt.Errorf("Unknown whisper method: %d", whisperMethod)
	}
}

func getMetric(w *whisper.Whisper, file, name string) (*mdata.ArchiveRequest, error) {
	if len(w.Header.Archives) == 0 {
		return nil, fmt.Errorf("Whisper file contains no archives: %q", file)
	}

	method, err := convertWhisperMethod(w.Header.Metadata.AggregationMethod)
	if err != nil {
		return nil, err
	}

	points := make(map[int][]whisper.Point)
	for i := range w.Header.Archives {
		p, err := w.DumpArchive(i)
		if err != nil {
			return nil, fmt.Errorf("Failed to dump archive %d from whisper file %s", i, file)
		}
		points[i] = p
	}

	res := &mdata.ArchiveRequest{
		MetricData: schema.MetricData{
			Name:     name,
			Value:    0,
			Interval: int(w.Header.Archives[0].SecondsPerPoint),
			Unit:     "unknown",
			Time:     0,
			Mtype:    "gauge",
			Tags:     []string{},
			OrgId:    *orgId,
		},
	}
	res.MetricData.SetId()
	mkey, err := schema.MKeyFromString(res.MetricData.Id)
	if err != nil {
		panic(err)
	}

	_, selectedSchema := schemas.Match(res.MetricData.Name, int(w.Header.Archives[0].SecondsPerPoint))
	conversion := newConversion(w.Header.Archives, points, method)
	for retIdx, retention := range selectedSchema.Retentions {
		convertedPoints := conversion.getPoints(retIdx, uint32(retention.SecondsPerPoint), uint32(retention.NumberOfPoints))
		for m, p := range convertedPoints {
			if len(p) == 0 {
				continue
			}

			var amkey schema.AMKey
			if retIdx == 0 {
				amkey = schema.AMKey{MKey: mkey}
			} else {
				amkey = schema.GetAMKey(mkey, m, retention.ChunkSpan)
			}

			encodedChunks := encodedChunksFromPoints(p, uint32(retention.SecondsPerPoint), retention.ChunkSpan)
			for _, chunk := range encodedChunks {
				res.ChunkWriteRequests = append(res.ChunkWriteRequests, mdata.NewChunkWriteRequest(
					nil,
					amkey,
					uint32(retention.MaxRetention()),
					chunk.Series.T0,
					chunk.Encode(retention.ChunkSpan),
					time.Now(),
				))
			}

			if res.MetricData.Time < int64(p[len(p)-1].Timestamp) {
				res.MetricData.Time = int64(p[len(p)-1].Timestamp)
			}
		}
	}

	return res, nil
}

func encodedChunksFromPoints(points []whisper.Point, intervalIn, chunkSpan uint32) []*chunk.Chunk {
	var point whisper.Point
	var t0, prevT0 uint32
	var c *chunk.Chunk
	var encodedChunks []*chunk.Chunk

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
			encodedChunks = append(encodedChunks, c)

			c = chunk.New(t0)
			prevT0 = t0
		}

		err := c.Push(point.Timestamp, point.Value)
		if err != nil {
			panic(fmt.Sprintf("ERROR: Failed to push value into chunk at t0 %d: %q", t0, err))
		}
	}

	// if the last written point was also the last one of the current chunk,
	// or if writeUnfinishedChunks is on, we close the chunk and push it
	if point.Timestamp == t0+chunkSpan-intervalIn || *writeUnfinishedChunks {
		c.Finish()
		encodedChunks = append(encodedChunks, c)
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
				log.Debugf("Skipping file %s with name %s", path, name)
				atomic.AddUint32(&skippedCount, 1)
				return nil
			}
			if len(path) < 4 || path[len(path)-4:] != ".wsp" {
				return nil
			}
			if pos != nil && pos.IsDone(path) {
				log.Debugf("Skipping file %s because it was listed as already done", path)
				return nil
			}

			fileChan <- path
			return nil
		},
	)

	close(fileChan)
}
