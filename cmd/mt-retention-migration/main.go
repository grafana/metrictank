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
		m.process(it)
	}

}

func (m *migrater) process(it *gocql.Iter) {
	var b []byte
	var ts int
	for it.Scan(&ts, &b) {
	}
}

func (m *migrater) write() {
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
	fmt.Fprintln(os.Stderr, msg)
}
