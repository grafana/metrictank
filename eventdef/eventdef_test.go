/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eventdef

import (
	"fmt"
	"github.com/raintank/raintank-metric/schema"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

var cmd *exec.Cmd
var esargs []string
var dataDir string
var stdout io.ReadCloser
var stderr io.ReadCloser

func init() {
	// if you change this, you need to change it in the config file too
	// This directory *will* be wiped out when the tests finish.
	dataDir = os.Getenv("ES_DATADIR")
	if dataDir == "" {
		dataDir = "/tmp/es_testing"
	}
	derr := os.Mkdir(dataDir, os.FileMode(0700))
	if derr != nil {
		panic(derr)
	}
	os.Mkdir(fmt.Sprintf("%s/log", dataDir), os.FileMode(0700))
	esEnv := os.Getenv("ES_COMMAND")
	if esEnv == "" {
		esargs = []string{"elasticsearch", "--config=test_conf/elasticsearch.yml"}
	} else {
		esargs = strings.Split(esEnv, " ")
	}
	verbose := os.Getenv("ES_TEST_VERBOSE")
	var cerr error
	cmd = exec.Command(esargs[0], esargs[1:]...)
	stderr, _ = cmd.StderrPipe()
	stdout, _ = cmd.StdoutPipe()
	if verbose != "" {
		go func() {
			for {
				w := make([]byte, 1024)
				_, rerr := stdout.Read(w)
				log.Printf("STDOUT: %s\n", string(w))
				if rerr != nil {
					break
				}
			}
		}()
		go func() {
			for {
				w := make([]byte, 1024)
				_, rerr := stderr.Read(w)
				log.Printf("STDERR: %s\n", string(w))
				if rerr != nil {
					break
				}
			}
		}()
	}
	cerr = cmd.Start()
	if cerr != nil {
		panic(cerr)
	}
	// wait for elasticsearch to start
	fin := make(chan struct{})
	go func() {
		for {
			_, conerr := net.Dial("tcp", "localhost:19200")
			if conerr == nil {
				fin <- struct{}{}
				break
			}
		}
	}()

	select {
	case <-fin:
		// indexing returned
	case <-time.After(30 * time.Second):
		log.Println("Starting elasticsearch timed out")
	}

	addr := "localhost:19200"
	err := InitElasticsearch(addr, "", "")
	if err != nil {
		panic(err)
	}
}

func makeEvent() *schema.ProbeEvent {
	e := new(schema.ProbeEvent)
	e.EventType = "monitor_state"
	e.OrgId = 1
	e.Severity = "ERROR"
	e.Source = "network_collector"
	e.Message = "100% packet loss"
	e.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)

	return e
}

// let's try pulling in an event and saving it, eh?

func TestSave(t *testing.T) {
	e := makeEvent()
	status := make(chan *BulkSaveStatus)
	err := Save(e, status)
	if err != nil {
		t.Errorf("error right off the bat with saving: %s", err.Error())
	}
	estat := <-status
	if estat.Requeue {
		t.Errorf("event status came back with 'requeue'")
	}
}

func TestMultipleSave(t *testing.T) {
	numEvents := 100
	done := make([]chan *BulkSaveStatus, numEvents)
	for i := 0; i < numEvents; i++ {
		e := makeEvent()
		status := make(chan *BulkSaveStatus, 1)
		err := Save(e, status)
		if err != nil {
			t.Errorf("error right off the bat for event %d with saving: %s", i, err.Error())
		}
		done[i] = status
	}
	var success int
	var failure int

	fin := make(chan int, 1)
	go func() {
		for _, s := range done {
			estat := <-s
			if estat.Requeue {
				failure++
			} else {
				success++
			}
		}
		fin <- 1
	}()
	select {
	case <-fin:
		// indexing returned
	case <-time.After(10 * time.Second):
		t.Errorf("Saving timed out")
	}
	if success < numEvents {
		t.Errorf("Did not save all events successfully: %d succeeded, %d failed", success, failure)
	}
}

func TestResubmitAll(t *testing.T) {
	// shut down elasticsearch here
	perr := cmd.Process.Signal(os.Interrupt)
	if perr != nil {
		panic(perr)
	}
	cmd.Wait()

	// be very sure elasticsearch stops
	fin := make(chan struct{}, 1)
	go func() {
		for {
			_, conerr := net.Dial("tcp", "localhost:19200")
			if conerr != nil {
				fin <- struct{}{}
				break
			}
		}
	}()

	select {
	case <-fin:
		// indexing returned
	case <-time.After(30 * time.Second):
		log.Println("Stopping elasticsearch timed out")
	}

	numEvents := 100
	done := make([]chan *BulkSaveStatus, numEvents)
	for i := 0; i < numEvents; i++ {
		e := makeEvent()
		status := make(chan *BulkSaveStatus, 1)
		err := Save(e, status)
		if err != nil {
			t.Errorf("error right off the bat for event %d with saving: %s", i, err.Error())
		}
		done[i] = status
	}
	var success int
	var failure int

	go func() {
		for _, s := range done {
			estat := <-s
			if estat.Requeue {
				failure++
			} else {
				success++
			}
		}
		fin <- struct{}{}
	}()
	select {
	case <-fin:
		// indexing returned
	case <-time.After(10 * time.Second):
		t.Errorf("Saving timed out")
	}
	if failure < numEvents {
		t.Errorf("Did not save all events successfully: %d succeeded, %d failed", success, failure)
	}
}

// check to see if the elasticsearch index has the right name, and clean up
func TestElasticSearchIndexes(t *testing.T) {
	esIndexPath := fmt.Sprintf("%s/elasticsearch_gotest/nodes/0/indices", dataDir)
	y, m, d := time.Now().Date()
	idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)
	dir, err := os.Open(esIndexPath)
	if err != nil {
		log.Printf("error opening %s: %s", esIndexPath, err.Error())
	}
	files, ferr := dir.Readdir(-1)
	if ferr != nil {
		log.Printf("error reading directory %s: %s", esIndexPath, ferr.Error())
	}
	var found bool
	for _, f := range files {
		if f.Name() == idxName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find index %s, but did not. Did find: %q", idxName, files)
	}
	err = os.RemoveAll(dataDir)
	if err != nil {
		log.Printf("Error removing %s: %s", esIndexPath, err.Error())
	}
}
