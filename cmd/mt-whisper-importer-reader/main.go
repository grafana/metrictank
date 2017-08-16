package main

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/metrictank/api"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/metrictank/mdata/chunk/archive"
	"gopkg.in/raintank/schema.v1"
)

var (
	exitOnError = flag.Bool(
		"exit-on-error",
		true,
		"Exit with a message when there's an error",
	)
	verbose = flag.Bool(
		"verbose",
		false,
		"Write logs to terminal",
	)
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
	chunkSpans   []uint32
	readArchives map[int]struct{}
	printLock    sync.Mutex
	schemas      conf.Schemas
)

func main() {
	var err error
	flag.Parse()

	schemas, err = conf.ReadSchemas(*dstSchemas)
	if err != nil {
		panic(fmt.Sprintf("Error when parsing schemas file: %q", err))
	}

	fileChan := make(chan string)

	wg := &sync.WaitGroup{}
	wg.Add(*threads)
	for i := 0; i < *threads; i++ {
		go processFromChan(fileChan, wg)
	}

	getFileListIntoChan(fileChan)
	wg.Wait()
}

func throwError(msg string) {
	msg = fmt.Sprintf("%s\n", msg)
	if *exitOnError {
		panic(msg)
	} else {
		printLock.Lock()
		fmt.Fprintln(os.Stderr, msg)
		printLock.Unlock()
	}
}

func log(msg string) {
	if *verbose {
		printLock.Lock()
		fmt.Println(msg)
		printLock.Unlock()
	}
}

func processFromChan(files chan string, wg *sync.WaitGroup) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: *insecureSSL},
	}
	client := &http.Client{Transport: tr}

	for file := range files {
		fd, err := os.Open(file)
		if err != nil {
			throwError(fmt.Sprintf("ERROR: Failed to open whisper file %q: %q\n", file, err))
			continue
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			throwError(fmt.Sprintf("ERROR: Failed to open whisper file %q: %q\n", file, err))
			continue
		}

		log(fmt.Sprintf("Processing file %q", file))
		mets, err := getMetrics(w, file)
		if err != nil {
			throwError(fmt.Sprintf("Failed to get metric: %q", err))
			continue
		}

		for _, met := range mets {
			b, err := met.MarshalCompressed()
			if err != nil {
				throwError(fmt.Sprintf("Failed to encode metric: %q", err))
				continue
			}

			req, err := http.NewRequest("POST", *httpEndpoint, io.Reader(b))
			if err != nil {
				panic(fmt.Sprintf("Cannot construct request to http endpoint %q: %q", *httpEndpoint, err))
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", "gzip")

			if len(*httpAuth) > 0 {
				req.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(*httpAuth)))
			}

			resp, err := client.Do(req)
			if err != nil {
				throwError(fmt.Sprintf("Error sending request to http endpoint %q: %q", *httpEndpoint, err))
				continue
			}
			if resp.StatusCode != 200 {
				throwError(fmt.Sprintf("Error when submitting data: %s", resp.Status))
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
	wg.Done()
}

// generate the metric name based on the file name and given prefix
func getMetricName(file string) string {
	// remove all leading '/' from file name
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

func shortAggMethodString(aggMethod whisper.AggregationMethod) string {
	switch aggMethod {
	case whisper.AggregationAverage:
		return "avg"
	case whisper.AggregationSum:
		return "sum"
	case whisper.AggregationMin:
		return "min"
	case whisper.AggregationMax:
		return "max"
	case whisper.AggregationLast:
		return "lst"
	default:
		return ""
	}
}

func getMetrics(w *whisper.Whisper, file string) ([]archive.Metric, error) {
	if len(w.Header.Archives) == 0 {
		return nil, errors.New(fmt.Sprintf("ERROR: Whisper file contains no archives: %q", file))
	}

	var metrics []archive.Metric
	var chunkSpan uint32
	var archiveInfo whisper.ArchiveInfo
	var archiveIdx int
	name := getMetricName(file)

	aggMethodStr := shortAggMethodString(w.Header.Metadata.AggregationMethod)
	if aggMethodStr == "" {
		return nil, errors.New(fmt.Sprintf(
			"ERROR: Aggregation method in file %s not allowed: %d(%s)\n",
			file,
			w.Header.Metadata.AggregationMethod,
			aggMethodStr,
		))
	}

	// md gets generated from the first archive in the whisper file
	md := getMetricData(name, int(w.Header.Archives[0].SecondsPerPoint))

	_, schema := schemas.Match(md.Name, 0)
	for retentionIdx, retention := range schema.Retentions {
		// retention TTL is the amount of historic data we're aiming to generate
		ttl := retention.SecondsPerPoint * retention.NumberOfPoints

		// on the first aggregation we determine the aggregation method as string
		// find the highest resolution archive that has enough data to fill the TTL
		// if none have enough, just take the last one
		for archiveIdx, archiveInfo = range w.Header.Archives {
			if archiveInfo.SecondsPerPoint*archiveInfo.Points >= uint32(ttl) {
				break
			}
		}

		points, err := w.DumpArchive(archiveIdx)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("ERROR: Failed to read archive %d in %q, skipping: %q", archiveIdx, file, err))
		}

		adjustedPoints := make(map[string][]whisper.Point)
		if uint32(retention.SecondsPerPoint) < archiveInfo.SecondsPerPoint {
			if retentionIdx == 0 || aggMethodStr != "avg" {
				// need to use aggregation as input for raw retention
				// if agg method is "avg" we want to actually calculate averages instead of sum & cnt
				adjustedPoints[aggMethodStr] = decResolution(points, aggMethodStr, archiveInfo.SecondsPerPoint, uint32(retention.SecondsPerPoint))
			} else {
				adjustedPoints["sum"] = decResolution(points, "sum", archiveInfo.SecondsPerPoint, uint32(retention.SecondsPerPoint))
				adjustedPoints["cnt"] = decResolution(points, "cnt", archiveInfo.SecondsPerPoint, uint32(retention.SecondsPerPoint))
			}
		} else if uint32(retention.SecondsPerPoint) > archiveInfo.SecondsPerPoint {
			adjustedPoints[aggMethodStr] = incResolution(points, archiveInfo.SecondsPerPoint, uint32(retention.SecondsPerPoint))
		} else {
			adjustedPoints[aggMethodStr] = sortPoints(points)
		}

		var archives []archive.Archive
		var rowKey string
		for method, points := range adjustedPoints {
			var point whisper.Point
			var t0, prevT0 uint32
			var c *chunk.Chunk
			var encodedChunks []chunk.IterGen

			for _, point = range points {
				// this shouldn't happen, but if it would we better catch it here because Metrictank wouldn't handle it well:
				// https://github.com/raintank/metrictank/blob/f1868cccfb92fc82cd853914af958f6d187c5f74/mdata/aggmetric.go#L378
				if point.Timestamp == 0 {
					continue
				}

				t0 = point.Timestamp - (point.Timestamp % chunkSpan)
				if prevT0 == 0 {
					c = chunk.New(t0)
					prevT0 = t0
				} else if prevT0 != t0 {
					log(fmt.Sprintf("Mark chunk at t0 %d as finished", prevT0))
					c.Finish()

					encodedChunks = append(encodedChunks, *chunk.NewBareIterGen(c.Bytes(), c.T0, chunkSpan))

					log(fmt.Sprintf("Create new chunk at t0 %d with chunk span %d", t0, chunkSpan))
					c = chunk.New(t0)
					prevT0 = t0
				}

				err := c.Push(point.Timestamp, point.Value)
				if err != nil {
					return nil, errors.New(fmt.Sprintf("ERROR: Failed to push value into chunk at t0 %d: %q", t0, err))
				}
			}

			if retentionIdx == 0 {
				if archiveIdx != 0 {
					//log.Warning("Using aggregated archive as input for raw archive")
					fmt.Println("Using aggregated archive as input for raw archive")
				}
				rowKey = md.Id
			} else {
				rowKey = api.AggMetricKey(
					md.Id,
					method,
					uint32(retention.SecondsPerPoint),
				)
			}

			// if the last written point was also the last one of the current chunk,
			// or if writeUnfinishedChunks is on, we close the chunk and push add it
			if point.Timestamp == t0+chunkSpan-archiveInfo.SecondsPerPoint || *writeUnfinishedChunks {
				log(fmt.Sprintf("Mark current (last) chunk at t0 %d as finished", t0))
				c.Finish()
				encodedChunks = append(encodedChunks, *chunk.NewBareIterGen(c.Bytes(), c.T0, chunkSpan))
			}

			log(fmt.Sprintf("Whisper file %q archive %d (%q) gets %d chunks", file, archiveIdx, name, len(encodedChunks)))
			archives = append(archives, archive.Archive{
				SecondsPerPoint: archiveInfo.SecondsPerPoint,
				Points:          archiveInfo.Points,
				Chunks:          encodedChunks,
				RowKey:          rowKey,
			})
		}
		metrics = append(metrics, archive.Metric{
			AggregationMethod: uint32(w.Header.Metadata.AggregationMethod),
			MetricData:        *md,
			Archives:          archives,
		})
	}

	return metrics, nil
}

// inreasing the resolution by just duplicating points to fill in empty data points
func incResolution(points []whisper.Point, inRes, outRes uint32) []whisper.Point {
	pointCount := (points[len(points)-1].Timestamp - points[0].Timestamp) * outRes / inRes
	out := make([]whisper.Point, 0, pointCount)
	for _, inPoint := range points {
		if inPoint.Timestamp == 0 {
			continue
		}

		if inPoint.Timestamp%outRes == 0 {
			out = append(out, inPoint)
		}

		for ts := inPoint.Timestamp + outRes - (inPoint.Timestamp % outRes); ts < inPoint.Timestamp+inRes; ts = ts + outRes {
			outPoint := inPoint
			outPoint.Timestamp = ts
			out = append(out, outPoint)
		}
	}
	return sortPoints(out)
}

// decreasing the resolution by using the aggregation method in aggMethod
func decResolution(points []whisper.Point, aggMethod string, inRes, outRes uint32) []whisper.Point {
	agg := mdata.NewAggregation()
	out := make([]whisper.Point, 0)
	currentBoundary := uint32(0)

	flush := func() {
		values := agg.FlushAndReset()
		if values["cnt"] == 0 {
			return
		}

		out = append(out, whisper.Point{
			Timestamp: currentBoundary,
			Value:     values[aggMethod],
		})
	}

	for _, inPoint := range sortPoints(points) {
		if inPoint.Timestamp == 0 {
			continue
		}
		boundary := mdata.AggBoundary(inPoint.Timestamp, outRes)

		if boundary == currentBoundary {
			agg.Add(inPoint.Value)
			if inPoint.Timestamp == boundary {
				flush()
			}
		} else {
			flush()
			currentBoundary = boundary
			agg.Add(inPoint.Value)
		}
	}

	return out
}

func getMetricData(name string, interval int) *schema.MetricData {
	md := &schema.MetricData{
		Name:     name,
		Metric:   name,
		Interval: interval,
		Value:    0,
		Unit:     "unknown",
		Time:     0,
		Mtype:    "gauge",
		Tags:     []string{},
		OrgId:    *orgId,
	}
	md.SetId()
	return md
}

// scan a directory and feed the list of whisper files relative to base into the given channel
func getFileListIntoChan(fileChan chan string) {
	filepath.Walk(
		*whisperDirectory,
		func(path string, info os.FileInfo, err error) error {
			if len(path) >= 4 && path[len(path)-4:] == ".wsp" {
				fileChan <- path
			}
			return nil
		},
	)

	close(fileChan)
}
