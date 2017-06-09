package main

import (
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/idx/cassandra"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

var (
	bufferSize       = 1000
	printLock        = sync.Mutex{}
	day_sec    int64 = 60 * 60 * 24
	startDay   int
)

type migrater struct {
	casIdx          *cassandra.CasIdx
	session         *gocql.Session
	chunkChan       chan *chunkDay
	metricCount     int
	readChunkCount  int
	writeChunkCount int
	ttlTables       mdata.TTLTables
	ttls            []uint32
	sourceTable     string
}

func main() {
	cassFlags := cassandra.ConfigSetup()
	cassFlags.IntVar(&startDay, "start-day", 0, "Day to start processing from")
	cassFlags.Parse(os.Args[1:])
	cassFlags.Usage = cassFlags.PrintDefaults

	cassandra.Enabled = true
	cluster.Init("migrator", "0", time.Now(), "", -1)
	cluster.Manager.SetPrimary(true)
	casIdx := cassandra.New()
	err := casIdx.InitBare()
	if err != nil {
		throwError(err.Error())
	}

	sourceTTL := uint32(60 * 60 * 24 * 68)
	sourceTTLTable := mdata.GetTTLTables(
		[]uint32{sourceTTL},
		20,
		mdata.Table_name_format,
	)
	fmt.Println(
		fmt.Sprintf("Using %s as source table", sourceTTLTable[sourceTTL].Table))

	// output retentions
	ttls := make([]uint32, 3)
	ttls[0] = 60 * 60 * 24 * 68
	ttls[1] = 60 * 60 * 24 * 365 * 3

	ttlTables := mdata.GetTTLTables(ttls, 20, mdata.Table_name_format)

	m := &migrater{
		casIdx:      casIdx,
		session:     casIdx.Session,
		chunkChan:   make(chan *chunkDay, bufferSize),
		ttlTables:   ttlTables,
		ttls:        ttls,
		sourceTable: sourceTTLTable[sourceTTL].Table,
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
	fmt.Println(fmt.Sprintf("Received %d metrics", len(defs)))

	for _, metric := range defs {
		m.processMetric(&metric)
		m.metricCount++
	}

	close(m.chunkChan)
}

func (m *migrater) processMetric(def *schema.MetricDefinition) {
	var b []byte
	var ts int
	var itgens []chunk.IterGen
	now := time.Now().Unix()
	start := (now - (int64(startDay) * day_sec))
	start_month := start / mdata.Month_sec
	end_month := (now - 1) / mdata.Month_sec

	fmt.Println(fmt.Sprintf("-- Processing metric with id %s and interval %d (months %d-%d)", def.Id, def.Interval, start_month, end_month))

	for month := start_month; month <= end_month; month++ {
		row_key := fmt.Sprintf("%s_%d", def.Id, month)
		itgenCount := 0
		for from := month * mdata.Month_sec; from < (month+1)*mdata.Month_sec; from += day_sec {
			fmt.Println(fmt.Sprintf("Day number %d", (from-(start_month*mdata.Month_sec))/day_sec))
			to := from + day_sec
			query := fmt.Sprintf(
				"SELECT ts, data FROM %s WHERE key = ? AND ts >= ? AND ts < ? ORDER BY ts ASC",
				m.sourceTable,
			)
			it := m.session.Query(query, row_key, from, to).Iter()
			for it.Scan(&ts, &b) {
				itgen, err := chunk.NewGen(b, uint32(ts))
				if err != nil {
					throwError(fmt.Sprintf("Error generating Itgen: %q", err))
				}
				itgens = append(itgens, *itgen)
			}
			err := it.Close()
			if err != nil {
				throwError(fmt.Sprintf("cassandra query error. %s", err))
			}
			itgenCount += len(itgens)
			m.generateChunks(itgens, def)
		}
		fmt.Println(fmt.Sprintf("Processed %d chunks of table %s for row_key %s", itgenCount, m.sourceTable, row_key))
	}
}

// represents one day of chunks of one metric aggregate
type chunkDay struct {
	tableName string
	id        string
	ttl       uint32
	itergens  []chunk.IterGen
}

func (m *migrater) generateChunks(itgens []chunk.IterGen, def *schema.MetricDefinition) {
	if len(itgens) == 0 {
		fmt.Println("0 chunks, nothing to do")
		return
	}

	m.readChunkCount += len(itgens)
	outChunkSpan := uint32(6 * 60 * 60)

	// if interval is larger than or equal to 1h we can directly write the chunk to
	// the higher retention table
	if def.Interval >= 60*60 {
		fmt.Println(fmt.Sprintf(
			"Interval is %dmin, directly writing to large interval table. Chunks:%d TTL: %d (%dd)",
			def.Interval/60, len(itgens), m.ttls[1], m.ttls[1]/(60*60*24),
		))

		m.chunkChan <- &chunkDay{
			itergens:  itgens,
			ttl:       m.ttls[1],
			tableName: m.ttlTables[m.ttls[1]].Table,
			id:        def.Id,
		}
		return
	}

	am := m.getAggMetric(def.Id, 60*60, m.ttls[1], outChunkSpan)
	for _, itgen := range itgens {
		iter, err := itgen.Get()
		if err != nil {
			throwError(
				fmt.Sprintf("Corrupt chunk %s at ts %d", def.Id, itgen.Ts),
			)
		}
		for iter.Next() {
			ts, val := iter.Values()
			am.Add(ts, val)
		}
	}
	fmt.Println(fmt.Sprintf("Generating 1h-rollups for %d Chunks", len(itgens)))
	m.writeRollup(am, 1)
}

func (m *migrater) writeRollup(am *mdata.AggMetric, ttlId int) {
	var aggs []string
	for _, agg := range am.GetAggregators() {
		for _, aggMetric := range agg.GetAggMetrics() {
			aggs = append(aggs, aggMetric.Key)
		}
	}
	fmt.Println(fmt.Sprintf("Processing rollups with keys %s", aggs))

	for _, agg := range am.GetAggregators() {
		for _, aggMetric := range agg.GetAggMetrics() {
			chunkCount := int(0)
			itgensNew := make([]chunk.IterGen, 0, len(am.Chunks))
			for _, c := range aggMetric.Chunks {
				if !c.Closed {
					c.Finish()
				}
				itgensNew = append(itgensNew, *chunk.NewBareIterGen(
					c.Bytes(),
					c.T0,
					aggMetric.ChunkSpan,
				))
			}
			chunkCount += len(itgensNew)

			rollupChunkDay := chunkDay{
				ttl:       m.ttls[ttlId],
				tableName: m.ttlTables[m.ttls[ttlId]].Table,
				id:        aggMetric.Key,
				itergens:  itgensNew,
			}
			m.chunkChan <- &rollupChunkDay
			fmt.Println(fmt.Sprintf(
				"Wrote rollup of %d chunks to table %s with ttl %d (%dd) for key %s",
				chunkCount, m.ttlTables[m.ttls[ttlId]].Table, m.ttls[ttlId], int64(m.ttls[ttlId])/day_sec, aggMetric.Key,
			))
		}
	}
}

func (m *migrater) getAggMetric(id string, rollupSpan int, ttl, outChunkSpan uint32) *mdata.AggMetric {
	return mdata.NewAggMetric(
		mdata.NewMockStore(),
		&cache.MockCache{},
		id,
		[]conf.Retention{
			conf.NewRetentionMT(1, outChunkSpan, outChunkSpan, 2, true),
			conf.NewRetentionMT(rollupSpan, ttl, outChunkSpan, uint32(day_sec)/outChunkSpan, true),
		},
		&conf.Aggregation{
			"default",
			regexp.MustCompile(".*"),
			0.5,
			// sum should not be necessary because that comes with Avg
			[]conf.Method{conf.Avg, conf.Lst, conf.Max, conf.Min},
		},
		false,
	)
}

func (m *migrater) write() {
	for {
		cd, more := <-m.chunkChan
		if !more {
			return
		}
		if len(cd.itergens) == 0 {
			continue
		}

		m.insertChunks(cd.tableName, cd.id, cd.ttl, cd.itergens)
		m.writeChunkCount++
	}
}

func (m *migrater) insertChunks(table, id string, ttl uint32, itergens []chunk.IterGen) {
	query := fmt.Sprintf("INSERT INTO %s (key, ts, data) values (?,?,?) USING TTL ? AND TIMESTAMP ?", table)
	for _, ig := range itergens {
		idMonth := fmt.Sprintf(
			"%s_%d",
			id,
			// itgens will always be of the same day for each chunkDay
			ig.Ts/mdata.Month_sec,
		)
		err := m.session.Query(
			query,
			idMonth,
			ig.Ts,
			mdata.PrepareChunkData(ig.Span, ig.Bytes()),
			ttl,
			ig.Ts,
		).Exec()
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
