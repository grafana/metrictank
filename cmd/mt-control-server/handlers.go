package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cmd/mt-control-server/controlmodels"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/macaron.v1"
)

var metrictankUrl string
var maxDefsPerMsg int
var indexConcurrency int

func ConfigHandlers() {
	FlagSet := flag.NewFlagSet("handlers", flag.ExitOnError)

	FlagSet.StringVar(&metrictankUrl, "metrictank-url", "http://metrictank:6060", "URL for MT cluster")
	FlagSet.IntVar(&maxDefsPerMsg, "max-defs-per-msg", 1000, "Maximum defs per control message")
	FlagSet.IntVar(&indexConcurrency, "index-concurrency", 5, "Concurrent queries to run against cassandra (per request)")

	globalconf.Register("handlers", FlagSet, flag.ExitOnError)
}

func tagsDelByQuery(ctx *macaron.Context, request controlmodels.IndexDelByQueryReq) {
	log.Infof("Received tagsDelByQuery: %v", request)

	// Translate to FindSeries
	findSeries := models.GraphiteTagFindSeries{
		Expr:   request.Expr,
		Limit:  request.Limit,
		To:     request.OlderThan,
		Meta:   true,
		Format: "full-json",
	}
	reqBody, err := json.Marshal(findSeries)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	// send to query address
	resp, err := http.Post(metrictankUrl+"/tags/findSeries", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Read in body so that the connection can be reused
		io.Copy(ioutil.Discard, resp.Body)
		response.Write(ctx, response.WrapError(fmt.Errorf("Request to cluster failed with status = %d", resp.StatusCode)))
		return
	}

	findSeriesResult := models.GraphiteTagFindSeriesFullResp{}
	err = json.NewDecoder(resp.Body).Decode(&findSeriesResult)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	retval := controlmodels.IndexDelByQueryResp{}
	retval.Count = len(findSeriesResult.Series)
	retval.Partitions = make(map[int]int)

	for _, s := range findSeriesResult.Series {
		retval.Partitions[int(s.Partition)]++
	}

	if request.Method != "dry-run" {
		op := schema.OpRemove
		if request.Method == "archive" {
			op = schema.OpArchive
		}

		flush := func(partition int32, cm *schema.ControlMsg) error {
			if len(cm.Defs) == 0 {
				return nil
			}
			cm.Op = op
			var b bytes.Buffer
			b.WriteByte(byte(msg.FormatIndexControlMessage))
			w := msgp.NewWriterSize(&b, 300)
			err := cm.EncodeMsg(w)
			if err != nil {
				return err
			}
			w.Flush()

			_, _, err = producer.producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Value:     sarama.ByteEncoder(b.Bytes()),
				Partition: partition,
			})
			return err
		}

		// Create message per partition
		partitionMsgs := make(map[int32]*schema.ControlMsg)
		for _, s := range findSeriesResult.Series {
			if _, ok := partitionMsgs[s.Partition]; !ok {
				partitionMsgs[s.Partition] = &schema.ControlMsg{}
			}
			cm := partitionMsgs[s.Partition]
			cm.Defs = append(cm.Defs, s)

			if len(cm.Defs) > maxDefsPerMsg {
				err = flush(s.Partition, cm)
				if err != nil {
					log.Warnf("Failed to produce control msg: err = %s", err)
				}
				partitionMsgs[s.Partition] = &schema.ControlMsg{}
			}
		}

		for partition, cm := range partitionMsgs {
			err = flush(partition, cm)
			if err != nil {
				log.Warnf("Failed to produce control msg: err = %s", err)
			}
		}
	}

	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewJson(200, retval, ""))
}

func tagsRestore(ctx *macaron.Context, request controlmodels.IndexRestoreReq) {
	log.Infof("Received tagsRestore: %v", request)

	// Pre-process expressions
	var nameMatch string
	var tagMatches []string
	for _, expr := range request.Expr {
		if strings.HasPrefix(expr, "name=") {
			nameMatch = strings.SplitN(expr, "=", 2)[1]
		} else {
			tagMatches = append(tagMatches, expr)
		}
	}

	numPartitions := request.NumPartitions
	if numPartitions < 1 {
		numPartitions = producer.numPartitions()
	}

	// For large archives, this could take quite a long time. Process asynchronously and
	// return response to know the job was kicked off. Provide an identifier to view logs.
	logId := "uninit"
	uuid, err := gocql.RandomUUID()
	if err == nil {
		logId = uuid.String()
	}

	scanPartition := func(partition int) {
		q := fmt.Sprintf("SELECT id, orgid, name, interval, unit, mtype, tags, lastupdate FROM %s WHERE partition = ?", cass.Config.ArchiveTable)

		var id, name, unit, mtype string
		var orgId, interval int
		var lastupdate int64
		var tags []string

		var defs []schema.MetricDefinition

		session := cass.Session.CurrentSession()
		iter := session.Query(q, partition).Iter()
	ITER:
		for iter.Scan(&id, &orgId, &name, &interval, &unit, &mtype, &tags, &lastupdate) {
			mkey, err := schema.MKeyFromString(id)
			if err != nil {
				log.Errorf("job=%s: load() could not parse ID %q: %s -> skipping", logId, id, err)
				continue
			}

			// Check if we care about this def
			if nameMatch != "" && name != nameMatch {
				continue
			}

		MATCH:
			for _, expr := range tagMatches {
				for _, tag := range tags {
					if tag == expr {
						continue MATCH
					}
				}
				continue ITER
			}

			if orgId < 0 {
				orgId = int(idx.OrgIdPublic)
			}

			defs = append(defs, schema.MetricDefinition{
				Id:         mkey,
				OrgId:      uint32(orgId),
				Partition:  int32(partition),
				Name:       name,
				Interval:   interval,
				Unit:       unit,
				Mtype:      mtype,
				Tags:       tags,
				LastUpdate: lastupdate,
			})
		}
		if err := iter.Close(); err != nil {
			log.Errorf("job=%s: could not close iterator for partition = %d: %s", logId, partition, err.Error())
		}

		log.Infof("job=%s: Restoring %d series from partition %d", logId, len(defs), partition)

		if len(defs) == 0 {
			return
		}

		// Chunk into batches
		var defBatches [][]schema.MetricDefinition
		for i := 0; i < len(defs); i += maxDefsPerMsg {
			end := i + maxDefsPerMsg

			if end > len(defs) {
				end = len(defs)
			}

			defBatches = append(defBatches, defs[i:end])
		}

		for _, batch := range defBatches {
			cm := &schema.ControlMsg{
				Defs: batch,
				Op:   schema.OpRestore,
			}

			var b bytes.Buffer
			b.WriteByte(byte(msg.FormatIndexControlMessage))
			w := msgp.NewWriterSize(&b, 300)
			err := cm.EncodeMsg(w)
			if err != nil {
				log.Warnf("job=%s: Failed to encode ControlMsg from partition = %d, err = %s", logId, partition, err)
				continue
			}
			w.Flush()

			_, _, err = producer.producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Value:     sarama.ByteEncoder(b.Bytes()),
				Partition: int32(partition),
			})
			if err != nil {
				log.Warnf("job=%s: Failed to send ControlMsg for partition = %d, err = %s", logId, partition, err)
				continue
			}
		}
	}

	go func() {
		partitions := make(chan int, indexConcurrency)
		var wg sync.WaitGroup
		for i := 0; i < indexConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for partition := range partitions {
					scanPartition(partition)
				}
			}()
		}

		for p := 0; p < numPartitions; p++ {
			partitions <- p
		}

		wg.Wait()
	}()

	retval := controlmodels.IndexRestoreResp{LogId: logId}
	response.Write(ctx, response.NewJson(200, retval, ""))
}
