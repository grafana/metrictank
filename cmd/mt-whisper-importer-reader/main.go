package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api"
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
	chunkSpanStr = flag.String(
		"chunkspans",
		"10min",
		"List of chunk spans separated by ':'. The 1st whisper archive gets the 1st span, 2nd the 2nd, etc",
	)
	namePrefix = flag.String(
		"name-prefix",
		"",
		"Prefix to prepend before every metric name, should include the '.' if necessary",
	)
	threads = flag.Int(
		"threads",
		10,
		"Number of workers threads to process .wsp files",
	)
	skipUnfinishedChunks = flag.Bool(
		"skip-unfinished-chunks",
		true,
		"Chunks that have not completed their chunk span should be skipped",
	)
	orgId = flag.Int(
		"orgid",
		1,
		"Organization ID the data belongs to ",
	)
	whisperDirectory = flag.String(
		"whisper-directory",
		"/opt/graphite/storage/whisper",
		"The directory that contains the whisper file structure",
	)
	readArchivesStr = flag.String(
		"read-archives",
		"*",
		"Comma separated list of positive integers or '*' for all archives",
	)
	chunkSpans   []uint32
	readArchives map[int]struct{}
	printLock    sync.Mutex
)

func main() {
	flag.Parse()

	for _, chunkSpanStrSplit := range strings.Split(*chunkSpanStr, ":") {
		chunkSpans = append(chunkSpans, dur.MustParseUNsec("chunkspan", chunkSpanStrSplit))
	}

	if *readArchivesStr != "*" {
		readArchives = make(map[int]struct{})
		for _, archiveIdStr := range strings.Split(*readArchivesStr, ",") {
			archiveId, err := strconv.Atoi(archiveIdStr)
			if err != nil {
				throwError(fmt.Sprintf("Invalid archive id %s: %s", archiveIdStr, err))
			}
			readArchives[archiveId] = struct{}{}
		}
	}

	fileChan := make(chan string)

	wg := &sync.WaitGroup{}
	for i := 0; i < *threads; i++ {
		go processFromChan(fileChan, wg)
	}

	getFileListIntoChan(fileChan, wg)
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
	client := &http.Client{}

	for file := range files {
		fd, err := os.Open(file)
		if err != nil {
			throwError(fmt.Sprintf("ERROR: Failed to open whisper file '%s': %s\n", file, err))
			continue
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			throwError(fmt.Sprintf("ERROR: Failed to open whisper file '%s': %s\n", file, err))
			continue
		}

		log(fmt.Sprintf("Processing file %s", file))
		b, err := getMetric(w, file).Encode()
		if err != nil {
			throwError(fmt.Sprintf("%s", err))
		}

		req, err := http.NewRequest("POST", *httpEndpoint, b)
		if err != nil {
			throwError(fmt.Sprintf("Cannot send request to http endpoint %s: %s", *httpEndpoint, err))
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")

		_, err = client.Do(req)
		if err != nil {
			throwError(fmt.Sprintf("Error sending request to http endpoint %s: %s", *httpEndpoint, err))
		}

		wg.Done()
	}
}

// generate the metric name based on the file name and given prefix
func getMetricName(file string) string {
	for {
		if file[0] == '/' {
			file = file[1:]
		} else {
			break
		}
	}
	splits := strings.Split(file, "/")
	leafNode := strings.Split(splits[len(splits)-1], ".")
	splits[len(splits)-1] = leafNode[0]
	return *namePrefix + strings.Join(splits, ".")
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

func getMetric(w *whisper.Whisper, file string) *archive.Metric {
	if len(w.Header.Archives) == 0 {
		throwError(fmt.Sprintf("ERROR: Whisper file contains no archives: %s", file))
	}

	archives := make([]archive.Archive, 0, len(w.Header.Archives))
	var chunkSpan uint32
	var base_id string
	name := getMetricName(file)

	for archiveIdx, archiveInfo := range w.Header.Archives {
		md := getMetricData(name, int(archiveInfo.SecondsPerPoint))

		if archiveIdx > 0 {
			md.Id = api.AggMetricKey(
				base_id,
				w.Header.Metadata.AggregationMethod.String(),
				archiveInfo.SecondsPerPoint,
			)
		} else {
			base_id = md.Id
		}

		// only read archive if archiveIdx is in readArchives
		if _, ok := readArchives[archiveIdx]; !ok && len(readArchives) > 0 {
			continue
		}

		encodedChunks := make(archive.ArchiveOfChunks)

		if len(chunkSpans)-1 < archiveIdx {
			// if we have more archives than chunk spans are specified, we simply use the last one
			chunkSpan = chunkSpans[len(chunkSpans)-1]
		} else {
			chunkSpan = chunkSpans[archiveIdx]
		}

		points, err := w.DumpArchive(archiveIdx)
		if err != nil {
			throwError(fmt.Sprintf("ERROR: Failed to read archive %d in '%s', skipping: %s", archiveIdx, file, err))
		}

		var point whisper.Point
		var t0, prevT0 uint32 = 0, 0
		var c *chunk.Chunk

		for _, point = range sortPoints(points) {
			if point.Timestamp == 0 {
				continue
			}

			t0 = point.Timestamp - (point.Timestamp % chunkSpan)
			if prevT0 == 0 {
				log(fmt.Sprintf("Create new chunk at t0: %d", t0))
				c = chunk.New(t0)
				prevT0 = t0
			} else if prevT0 != t0 {
				log(fmt.Sprintf("Mark chunk at t0 %d as finished", prevT0))
				c.Finish()

				encodedChunks[c.T0] = archive.MetricChunk{
					ChunkSpan: chunkSpan,
					Bytes:     c.Series.Bytes(),
				}

				log(fmt.Sprintf("Create new chunk at t0: %d", t0))
				c = chunk.New(t0)
				prevT0 = t0
			}

			err := c.Push(point.Timestamp, point.Value)
			if err != nil {
				throwError(fmt.Sprintf("ERROR: Failed to push value into chunk at t0 %d: %s", t0, err))
			}
		}

		if int64(point.Timestamp) > md.Time {
			md.Time = int64(point.Timestamp)
		}

		// if the last written point was also the last one of the current chunk,
		// or if skipUnfinishedChunks is on, we close the chunk and
		if point.Timestamp == t0+chunkSpan-archiveInfo.SecondsPerPoint || !*skipUnfinishedChunks {
			log(fmt.Sprintf("Mark current (last) chunk at t0 %d as finished", t0))
			c.Finish()
			encodedChunks[c.T0] = archive.MetricChunk{
				ChunkSpan: chunkSpan,
				Bytes:     c.Series.Bytes(),
			}
		}

		log(fmt.Sprintf("Whisper file %s archive %d (%s) gets %d chunks", file, archiveIdx, name, len(encodedChunks)))
		archives = append(archives, archive.Archive{
			ArchiveInfo: archiveInfo,
			Chunks:      encodedChunks,
			MetricData:  *md,
		})
	}

	return &archive.Metric{
		Metadata: w.Header.Metadata,
		Archives: archives,
	}
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
func getFileListIntoChan(fileChan chan string, wg *sync.WaitGroup) {

	filepath.Walk(
		*whisperDirectory,
		func(path string, info os.FileInfo, err error) error {
			if path[len(path)-4:] == ".wsp" {
				wg.Add(1)
				fileChan <- path
			}
			return nil
		},
	)

	close(fileChan)
}
