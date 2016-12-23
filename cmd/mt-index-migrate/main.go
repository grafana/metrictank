package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

const table_schema = `CREATE TABLE IF NOT EXISTS %s.metric_idx (
    id text,
    orgId int,
    partition int,
    name text,
    metric text,
    interval int,
    unit text,
    mtype text,
    tags set<text>,
    lastupdate int,
    PRIMARY KEY (id, partition)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}`
const metric_idx_index = `CREATE INDEX IF NOT EXISTS ON %s.metric_idx(partition)`

var (
	dryRun        = flag.Bool("dry-run", true, "run in dry-run mode. No changes will be made.")
	logLevel      = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	cassAddr      = flag.String("cass-addr", "localhost", "Address of cassandra host.")
	keyspace      = flag.String("keyspace", "raintank", "Cassandra keyspace to use.")
	partitionBy   = flag.String("partition-by", "byOrg", "method used for paritioning metrics. (byOrg|bySeries)")
	numPartitions = flag.Int("num-partitions", 1, "number of partitions in cluster")

	wg sync.WaitGroup
)

func main() {
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, *logLevel))

	defsChan := make(chan *schema.MetricDefinition, 100)

	cluster := gocql.NewCluster(*cassAddr)
	cluster.Consistency = gocql.ParseConsistency("one")
	cluster.Timeout = time.Second
	cluster.NumConns = 2
	cluster.ProtoVersion = 4
	cluster.Keyspace = *keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(4, "failed to create cql session. %s", err)
	}

	// ensure the new table exists.
	err = session.Query(fmt.Sprintf(table_schema, *keyspace)).Exec()
	if err != nil {
		log.Fatal(4, "cassandra-idx failed to initialize cassandra table. %s", err)
	}
	err = session.Query(fmt.Sprintf(metric_idx_index, *keyspace)).Exec()
	if err != nil {
		log.Fatal(4, "cassandra-idx failed to initialize cassandra index. %s", err)
	}

	wg.Add(1)
	go writeDefs(session, defsChan)
	wg.Add(1)
	go getDefs(session, defsChan)

	wg.Wait()

}

func writeDefs(session *gocql.Session, defsChan chan *schema.MetricDefinition) {
	log.Info("starting write thread")
	defer wg.Done()
	counter := 0
	pre := time.Now()
	for def := range defsChan {
		qry := `INSERT INTO metric_idx (id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if *dryRun {
			fmt.Printf(
				"INSERT INTO metric_idx (id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate) VALUES ('%s', '%d', '%d','%s', '%s','%d', '%s','%s', '%v', '%d')\n",
				def.Id,
				def.OrgId,
				def.Partition,
				def.Name,
				def.Metric,
				def.Interval,
				def.Unit,
				def.Mtype,
				def.Tags,
				def.LastUpdate)
			continue
		}
		success := false
		attempts := 0
		for !success {
			if err := session.Query(
				qry,
				def.Id,
				def.OrgId,
				def.Partition,
				def.Name,
				def.Metric,
				def.Interval,
				def.Unit,
				def.Mtype,
				def.Tags,
				def.LastUpdate).Exec(); err != nil {

				if (attempts % 20) == 0 {
					log.Warn("cassandra-idx Failed to write def to cassandra. it will be retried. %s", err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
				log.Debug("cassandra-idx metricDef saved to cassandra. %s", def.Id)
				counter++
			}
		}
	}
	log.Info("Inserted %d metricDefs in %s", counter, time.Since(pre).String())
}

func getDefs(session *gocql.Session, defsChan chan *schema.MetricDefinition) {
	log.Info("starting read thread")
	defer wg.Done()
	defer close(defsChan)
	partitioner, err := cluster.NewKafkaPartitioner(*partitionBy)
	if err != nil {
		log.Fatal(4, "failed to initialize partitioner. %s", err)
	}
	iter := session.Query("SELECT def from metric_def_idx").Iter()

	var data []byte
	for iter.Scan(&data) {
		mdef := schema.MetricDefinition{}
		_, err := mdef.UnmarshalMsg(data)
		if err != nil {
			log.Error(3, "cassandra-idx Bad definition in index. %s - %s", data, err)
			continue
		}
		log.Debug("retrieved %s from old index.", mdef.Id)
		if *numPartitions == 1 {
			mdef.Partition = 0
		} else {
			p, err := partitioner.Partition(&mdef, int32(*numPartitions))
			if err != nil {
				log.Error(3, "failed to get partition id of metric. %s", err)
				mdef.Partition = 0
			} else {
				mdef.Partition = p
			}
		}
		defsChan <- &mdef
	}
}
