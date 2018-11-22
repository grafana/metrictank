package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/util"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

var (
	logLevel        = flag.String("log-level", "info", "log level. panic|fatal|error|warning|info|debug")
	dryRun          = flag.Bool("dry-run", true, "run in dry-run mode. No changes will be made.")
	srcCassAddr     = flag.String("src-cass-addr", "localhost", "Address of cassandra host to migrate from.")
	dstCassAddr     = flag.String("dst-cass-addr", "localhost", "Address of cassandra host to migrate to.")
	srcKeyspace     = flag.String("src-keyspace", "raintank", "Cassandra keyspace in use on source.")
	dstKeyspace     = flag.String("dst-keyspace", "raintank", "Cassandra keyspace in use on destination.")
	partitionScheme = flag.String("partition-scheme", "byOrg", "method used for partitioning metrics. (byOrg|bySeries|bySeriesWithTags)")
	numPartitions   = flag.Int("num-partitions", 1, "number of partitions in cluster")
	schemaFile      = flag.String("schema-file", "/etc/metrictank/schema-idx-cassandra.toml", "File containing the needed schemas in case database needs initializing")

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

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("failed to parse log-level, %s", err.Error())
	}
	log.SetLevel(lvl)
	log.Infof("logging level set to '%s'", *logLevel)

	defsChan := make(chan *schema.MetricDefinition, 100)

	srcCluster := gocql.NewCluster(*srcCassAddr)
	srcCluster.Consistency = gocql.ParseConsistency("one")
	srcCluster.Timeout = time.Second
	srcCluster.NumConns = 2
	srcCluster.ProtoVersion = 4
	srcCluster.Keyspace = *srcKeyspace
	srcSession, err := srcCluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to create cql session for source cassandra. %s", err.Error())
	}
	dstCluster := gocql.NewCluster(*dstCassAddr)
	dstCluster.Consistency = gocql.ParseConsistency("one")
	dstCluster.Timeout = time.Second
	dstCluster.NumConns = 2
	dstCluster.ProtoVersion = 4
	dstCluster.Keyspace = *dstKeyspace
	dstSession, err := dstCluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to create cql session for destination cassandra. %s", err.Error())
	}

	// ensure the dest table exists.
	schemaTable := util.ReadEntry(*schemaFile, "schema_table").(string)
	err = dstSession.Query(fmt.Sprintf(schemaTable, *dstKeyspace)).Exec()
	if err != nil {
		log.Fatalf("cassandra-idx failed to initialize cassandra table. %s", err.Error())
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
		qry := `INSERT INTO metric_idx (id, orgid, partition, name, interval, unit, mtype, tags, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if *dryRun {
			fmt.Printf(
				"INSERT INTO metric_idx (id, orgid, partition, name, interval, unit, mtype, tags, lastupdate) VALUES ('%s', '%d', '%d','%s', '%d', '%s','%s', '%v', '%d')\n",
				def.Id,
				def.OrgId,
				def.Partition,
				def.Name,
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
				def.Interval,
				def.Unit,
				def.Mtype,
				def.Tags,
				def.LastUpdate).Exec(); err != nil {

				if (attempts % 20) == 0 {
					log.Warnf("cassandra-idx Failed to write def to cassandra. it will be retried. %s", err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
				log.Debugf("cassandra-idx metricDef saved to cassandra. %s", def.Id)
				counter++
			}
		}
	}
	log.Infof("Inserted %d metricDefs in %s", counter, time.Since(pre).String())
}

func getDefs(session *gocql.Session, defsChan chan *schema.MetricDefinition) {
	log.Info("starting read thread")
	defer wg.Done()
	defer close(defsChan)
	partitioner, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		log.Fatalf("failed to initialize partitioner. %s", err.Error())
	}
	iter := session.Query("SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from metric_idx").Iter()

	var id, name, unit, mtype string
	var orgId, interval int
	var partition int32
	var lastupdate int64
	var tags []string
	for iter.Scan(&id, &orgId, &partition, &name, &interval, &unit, &mtype, &tags, &lastupdate) {
		mkey, err := schema.MKeyFromString(id)
		if err != nil {
			log.Errorf("could not parse ID %q: %s -> skipping", id, err.Error())
			continue
		}
		mdef := schema.MetricDefinition{
			Id:         mkey,
			OrgId:      uint32(orgId),
			Partition:  partition,
			Name:       name,
			Interval:   interval,
			Unit:       unit,
			Mtype:      mtype,
			Tags:       tags,
			LastUpdate: lastupdate,
		}
		log.Debugf("retrieved %s from old index.", mdef.Id)
		if *numPartitions == 1 {
			mdef.Partition = 0
		} else {
			p, err := partitioner.Partition(&mdef, int32(*numPartitions))
			if err != nil {
				log.Fatalf("failed to get partition id of metric. %s", err.Error())
			} else {
				mdef.Partition = p
			}
		}
		defsChan <- &mdef
	}
}
