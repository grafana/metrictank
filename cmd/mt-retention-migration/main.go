package main

import (
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/raintank/metrictank/idx/cassandra"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
	"os"
	//"strings"
	"sync"
	"time"
)

var (
	bufferSize = flag.Int(
		"buffer-size",
		1000,
		"number of chunks to buffer before reading blocks and waits for write",
	)
	printLock = sync.Mutex{}
)

type migrater struct {
	casIdx          *cassandra.CasIdx
	session         *gocql.Session
	chunkChan       chan *chunkWithMeta
	metricCount     int
	readChunkCount  int
	writeChunkCount int
}

type chunkWithMeta struct {
	tableName string
	id        string
	ttl       uint32
	itergens  []chunk.IterGen
}

func main() {
	cassandra.ConfigSetup()
	cassandra.Enabled = true
	casIdx := cassandra.New()
	err := casIdx.InitBare()
	throwError(err.Error())

	m := &migrater{
		casIdx:    casIdx,
		session:   casIdx.Session,
		chunkChan: make(chan *chunkWithMeta, *bufferSize),
	}

	m.Start()
}

func (m *migrater) Start() {
	go m.read()
	m.write()
	printLock.Lock()
	fmt.Println(
		fmt.Sprintf(
			"Finished. Metrics: %d, Read chunks: %d, Wrote chunks: %d",
			m.metricCount,
			m.readChunkCount,
			m.writeChunkCount,
		),
	)
	printLock.Unlock()
}

func (m *migrater) read() {
	defs := m.casIdx.Load(nil)

	for _, metric := range defs {
		m.processMetric(&metric)
		m.metricCount++
	}
}

func (m *migrater) processMetric(def *schema.MetricDefinition) {
	now := time.Now().Unix()
	start_month := now % mdata.Month_sec
	end_month := (now - 1) - ((now - 1) % mdata.Month_sec)

	for month := start_month; month <= end_month; month++ {
		row_key := fmt.Sprintf("%s_%d", def.Id, start_month/mdata.Month_sec)
		query := fmt.Sprintf(
			"SELECT ts, data FROM %s WHERE key = ? AND ts > ? AND ts < ? ORDER BY ts ASC",
			row_key,
		)
		it := m.session.Query(query).Iter()
		m.generateChunks(m.process(it), def)
	}
}

func (m *migrater) process(it *gocql.Iter) []chunk.Iter {
	var b []byte
	var ts int
	var iters []chunk.Iter

	for it.Scan(&ts, &b) {
		itgen, err := chunk.NewGen(b, uint32(ts))
		if err != nil {
			throwError(fmt.Sprintf("Error generating Itgen: %q", err))
		}

		iter, err := itgen.Get()
		if err != nil {
			throwError(fmt.Sprintf("itergen: error getting iter %+v", err))
			continue
		}
		iters = append(iters, *iter)
		m.readChunkCount++
	}

	return iters
}

func (m *migrater) generateChunks(iters []chunk.Iter, def *schema.MetricDefinition) {
	c := chunkWithMeta{}
	for _, iter := range iters {
		for iter.Next() {
			//ts, val := iter.Values()
		}
	}
	m.chunkChan <- &c
}

func (m *migrater) write() {
	for {
		chunk, more := <-m.chunkChan
		if !more {
			return
		}

		m.insertChunks(chunk.tableName, chunk.id, chunk.ttl, chunk.itergens)
		m.writeChunkCount++
	}
}

func (m *migrater) insertChunks(table, id string, ttl uint32, itergens []chunk.IterGen) {
	query := fmt.Sprintf("INSERT INTO %s (key, ts, data) values (?,?,?) USING TTL %d", table, ttl)
	for _, ig := range itergens {
		rowKey := fmt.Sprintf("%s_%d", id, ig.Ts/mdata.Month_sec)
		err := m.session.Query(query, rowKey, ig.Ts, mdata.PrepareChunkData(ig.Span, ig.Bytes())).Exec()
		if err != nil {
			throwError(fmt.Sprintf("Error in query: %q", err))
		}
	}
}

func throwError(msg string) {
	msg = fmt.Sprintf("%s\n", msg)
	printLock.Lock()
	fmt.Fprintln(os.Stderr, msg)
	printLock.Unlock()
}
