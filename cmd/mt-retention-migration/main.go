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
	printLock    = sync.Mutex{}
	source_table = "metrics_1024"
)
var day_sec int64 = 60 * 60 * 24

type migrater struct {
	casIdx          *cassandra.CasIdx
	session         *gocql.Session
	chunkChan       chan *chunkDay
	metricCount     int
	readChunkCount  int
	writeChunkCount int
	ttlTables       mdata.TTLTables
	ttls            []uint32
}

type chunkDay struct {
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

	ttls := make([]uint32, 3)
	// 1s:1d:1h:2,1m:60d:6h:2,30m:3y:6h:2
	ttls[0] = 60 * 60 * 24
	ttls[1] = 60 * 60 * 24 * 60
	ttls[2] = 60 * 60 * 24 * 365 * 3

	ttlTables := mdata.GetTTLTables(
		ttls,
		20,
		mdata.Table_name_format,
	)

	m := &migrater{
		casIdx:    casIdx,
		session:   casIdx.Session,
		chunkChan: make(chan *chunkDay, *bufferSize),
		ttlTables: ttlTables,
		ttls:      ttls,
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
	start := (now - (68 * day_sec))
	start_month := start / mdata.Month_sec
	end_month := (now - 1) / mdata.Month_sec

	for month := start_month; month <= end_month; month++ {
		row_key := fmt.Sprintf("%s_%d", def.Id, start_month)
		for from := start_month * mdata.Month_sec; from <= month+(28*day_sec); from += day_sec {
			to := from + day_sec
			query := fmt.Sprintf(
				"SELECT ts, data FROM %s WHERE key = ? AND ts > ? AND ts <= ? ORDER BY ts ASC",
				source_table,
			)
			it := m.session.Query(query, row_key, from, to).Iter()
			m.generateChunks(m.process(it), def)
		}
	}
}

func (m *migrater) process(it *gocql.Iter) []chunk.IterGen {
	var b []byte
	var ts int
	var itgens []chunk.IterGen

	for it.Scan(&ts, &b) {
		itgen, err := chunk.NewGen(b, uint32(ts))
		if err != nil {
			throwError(fmt.Sprintf("Error generating Itgen: %q", err))
		}

		itgens = append(itgens, *itgen)
		m.readChunkCount++
	}

	return itgens
}

func (m *migrater) generateChunks(itgens []chunk.IterGen, def *schema.MetricDefinition) {
	c := chunkDay{}

	// if interval is larger than 30min we can directly write the chunk to
	// the highest retention table
	if def.Interval > 60*30 {
		c.itergens = itgens
		c.ttl = m.ttls[2]
		c.tableName = m.ttlTables[m.ttls[2]].Table
		c.id = def.Id

		m.chunkChan <- &c
		return
	}

	if def.Interval < 60 {
		// create one min rollup
	}

	// chunks older than 60 days can be dropped
	dropBefore := uint32(time.Now().Unix() - 60*60*24*60)
	for _, itgen := range itgens {
		if itgen.Ts < dropBefore {
			continue
		}

		/*if itgen.Ts < noRawBefore {
			continue
		}
		ts, val := iter.Values()*/
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
