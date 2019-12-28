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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata/importer"
	"github.com/kisielk/whisper-go/whisper"
	log "github.com/sirupsen/logrus"
)

var (
	httpEndpoint = flag.String(
		"http-endpoint",
		"http://127.0.0.1:8080/metrics/import",
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
	importUntil = flag.Uint(
		"import-until",
		math.MaxUint32,
		"Only import up to, but not including, the specified timestamp",
	)
	importFrom = flag.Uint(
		"import-from",
		0,
		"Only import starting from the specified timestamp",
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
	customHeadersString = flag.String(
		"custom-headers",
		"",
		"headers to add to every request, in the format \"<name>:<value>;<name>:<value>\"",
	)
	schemas        conf.Schemas
	nameFilter     *regexp.Regexp
	processedCount uint32
	skippedCount   uint32
	customHeaders  map[string]string
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

	if len(*customHeadersString) > 0 {
		customHeaders = make(map[string]string)
		for _, header := range strings.Split(*customHeadersString, ";") {
			splits := strings.SplitN(header, ":", 2)
			if len(splits) < 2 {
				panic(fmt.Sprintf("Invalid custom headers specified: %q", *customHeadersString))
			}
			customHeaders[splits[0]] = splits[1]
		}
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
		data, err := importer.NewArchiveRequest(w, schemas, file, name, uint32(*importFrom), uint32(*importUntil), *writeUnfinishedChunks)
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
			for name, value := range customHeaders {
				req.Header.Set(name, value)
			}

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
