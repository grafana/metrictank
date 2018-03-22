package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var (
	dryRun          = flag.Bool("dry-run", true, "run in dry-run mode. No changes will be made.")
	logLevel        = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	srcCassAddr     = flag.String("src-cass-addr", "localhost", "Address of cassandra host to migrate from.")
	dstCassAddr     = flag.String("dst-cass-addr", "localhost", "Address of cassandra host to migrate to.")
	srcKeyspace     = flag.String("src-keyspace", "raintank", "Cassandra keyspace in use on source.")
	dstKeyspace     = flag.String("dst-keyspace", "raintank", "Cassandra keyspace in use on destination.")
	partitionScheme = flag.String("partition-scheme", "byOrg", "method used for partitioning metrics. (byOrg|bySeries)")
	numPartitions   = flag.Int("num-partitions", 1, "number of partitions in cluster")

	wg sync.WaitGroup
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-index-migrate")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Migrate metric index from one cassandra keyspace to another.")
		fmt.Fprintln(os.Stderr, "This tool can be used for moving data to a different keyspace or cassandra cluster")
		fmt.Fprintln(os.Stderr, "or for resetting partition information when the number of partitions being used has changed.")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, *logLevel))

	defsChan := make(chan *schema.MetricDefinition, 100)

	srcCluster := gocql.NewCluster(*srcCassAddr)
	srcCluster.Consistency = gocql.ParseConsistency("one")
	srcCluster.Timeout = time.Second
	srcCluster.NumConns = 2
	srcCluster.ProtoVersion = 4
	srcCluster.Keyspace = *srcKeyspace
	srcSession, err := srcCluster.CreateSession()
	if err != nil {
		log.Fatal(4, "failed to create cql session for source cassandra. %s", err)
	}
	dstCluster := gocql.NewCluster(*dstCassAddr)
	dstCluster.Consistency = gocql.ParseConsistency("one")
	dstCluster.Timeout = time.Second
	dstCluster.NumConns = 2
	dstCluster.ProtoVersion = 4
	dstCluster.Keyspace = *dstKeyspace
	dstSession, err := dstCluster.CreateSession()
	if err != nil {
		log.Fatal(4, "failed to create cql session for destination cassandra. %s", err)
	}

	// ensure the dest table exists.
	err = dstSession.Query(fmt.Sprintf(cassandra.TableSchema, *dstKeyspace)).Exec()
	if err != nil {
		log.Fatal(4, "cassandra-idx failed to initialize cassandra table. %s", err)
	}

	wg.Add(1)
	go writeDefs(dstSession, defsChan)
	wg.Add(1)
	go getDefs(srcSession, defsChan)

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
	partitioner, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		log.Fatal(4, "failed to initialize partitioner. %s", err)
	}
	iter := session.Query("SELECT id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate from metric_idx").Iter()

	var id, name, metric, unit, mtype string
	var orgId, interval int
	var partition int32
	var lastupdate int64
	var tags []string
	for iter.Scan(&id, &orgId, &partition, &name, &metric, &interval, &unit, &mtype, &tags, &lastupdate) {
		mkey, err := schema.MKeyFromString(id)
		if err != nil {
			log.Error(3, "could not parse ID %q: %s -> skipping", id, err)
			continue
		}
		mdef := schema.MetricDefinition{
			Id:         mkey,
			OrgId:      orgId,
			Partition:  partition,
			Name:       name,
			Metric:     metric,
			Interval:   interval,
			Unit:       unit,
			Mtype:      mtype,
			Tags:       tags,
			LastUpdate: lastupdate,
		}
		log.Debug("retrieved %s from old index.", mdef.Id)
		if *numPartitions == 1 {
			mdef.Partition = 0
		} else {
			p, err := partitioner.Partition(&mdef, int32(*numPartitions))
			if err != nil {
				log.Fatal(4, "failed to get partition id of metric. %s", err)
			} else {
				mdef.Partition = p
			}
		}
		defsChan <- &mdef
	}
}
