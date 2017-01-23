package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/mattbaird/elastigo/lib"
	schemaV0 "gopkg.in/raintank/schema.v0"
	"gopkg.in/raintank/schema.v1"
)

var (
	dryRun       = flag.Bool("dry-run", true, "run in dry-run mode. No changes will be made.")
	cassAddr     = flag.String("cass-addr", "localhost", "Address of cassandra host.")
	cassKeyspace = flag.String("keyspace", "raintank", "Cassandra keyspace to use.")
	esAddr       = flag.String("es-addr", "localhost", "address of elasticsearch host.")
	esIndex      = flag.String("index", "metric", "elasticsearch index that contains current metric index values.")

	wg sync.WaitGroup
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-index-migrate-050-to054")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Converts a metrictank index to the new 0.5.4 (and beyond) format, using cassandra instead of elasticsearch")
		fmt.Fprintln(os.Stderr, "differences:")
		fmt.Fprintln(os.Stderr, " * schema 0 to schema 1 - proper metrics2.0 - (for 0.5.1, 576d8fcb47888b8a334e9a125d6aadf8e0e4d4d7)")
		fmt.Fprintln(os.Stderr, " * store data in cassandra in the metric_def_ix table, as a messagepack encoded blob (cassandra idx new in 0.5.3 - 26be821bd8bead43db120e96d14d0ee88d6b6880)")
		fmt.Fprintln(os.Stderr, " * use lastUpdate field (for 0.5.4, a07007ab8d4a22b122bbc5f9fadb51480e1c5b0c)")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	defsChan := make(chan *schema.MetricDefinition, 100)

	cluster := gocql.NewCluster(*cassAddr)
	cluster.Consistency = gocql.ParseConsistency("one")
	cluster.Timeout = time.Second
	cluster.NumConns = 10
	cluster.ProtoVersion = 4
	cluster.Keyspace = *cassKeyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to create cql session. %s", err)
	}
	wg.Add(1)
	go writeDefs(session, defsChan)

	conn := elastigo.NewConn()
	conn.SetHosts([]string{*esAddr})
	wg.Add(1)
	go getDefs(conn, defsChan)

	wg.Wait()

}

func writeDefs(session *gocql.Session, defsChan chan *schema.MetricDefinition) {
	defer wg.Done()
	data := make([]byte, 0)
	for def := range defsChan {
		data = data[:0]
		data, err := def.MarshalMsg(data)
		if err != nil {
			log.Printf("Failed to marshal metricDef. %s", err)
			continue
		}
		if *dryRun {
			fmt.Printf("INSERT INTO metric_def_idx (id, def) VALUES ('%s', '%s')\n", def.Id, data)
			continue
		}
		success := false
		attempts := 0
		for !success {
			if err := session.Query(`INSERT INTO metric_def_idx (id, def) VALUES (?, ?)`, def.Id, data).Exec(); err != nil {
				if (attempts % 20) == 0 {
					log.Printf("cassandra-idx Failed to write def to cassandra. it will be retried. %s", err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
			}
		}
	}
	log.Printf("defsWriter exiting.")
}

func getDefs(conn *elastigo.Conn, defsChan chan *schema.MetricDefinition) {
	defer wg.Done()
	defer close(defsChan)
	var err error
	var out elastigo.SearchResult
	loading := true
	scroll_id := ""
	for loading {
		if scroll_id == "" {
			out, err = conn.Search(*esIndex, "metric_index", map[string]interface{}{"scroll": "1m", "size": 1000}, nil)
		} else {
			out, err = conn.Scroll(map[string]interface{}{"scroll": "1m"}, scroll_id)
		}
		if err != nil {
			log.Fatalf("Failed to load metric definitions from ES. %s", err)
		}
		for _, h := range out.Hits.Hits {
			mdef, err := schemaV0.MetricDefinitionFromJSON(*h.Source)
			if err != nil {
				log.Printf("Error: Bad definition in index. %v - %s", h.Source, err)
				continue
			}
			newDef := &schema.MetricDefinition{
				Id:         mdef.Id,
				OrgId:      mdef.OrgId,
				Name:       mdef.Name,
				Metric:     mdef.Metric,
				Interval:   mdef.Interval,
				Unit:       mdef.Unit,
				Mtype:      mdef.TargetType,
				Tags:       mdef.Tags,
				LastUpdate: mdef.LastUpdate,
			}
			defsChan <- newDef
		}

		scroll_id = out.ScrollId
		if out.Hits.Len() == 0 {
			loading = false
		}
	}
}
