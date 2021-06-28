package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cmd/mt-control-server/controlmodels"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/macaron.v1"
)

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
	resp, err := http.Post(*cluster+"/tags/findSeries", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Read in body so that the connection can be reused
		io.Copy(ioutil.Discard, resp.Body)
		response.Write(ctx, response.WrapError(errors.Errorf("Request to cluster failed with status = %d", resp.StatusCode)))
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

		// TODO - configurable?
		MAX_DEFS_PER_MSG := 1000

		// Create message per partition
		partitionMsgs := make(map[int32]*schema.ControlMsg)
		for _, s := range findSeriesResult.Series {
			if _, ok := partitionMsgs[s.Partition]; !ok {
				partitionMsgs[s.Partition] = &schema.ControlMsg{}
			}
			cm := partitionMsgs[s.Partition]
			cm.Defs = append(cm.Defs, s)

			if len(cm.Defs) > MAX_DEFS_PER_MSG {
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
	// crawl archive for matches

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
				log.Errorf("cassandra: load() could not parse ID %q: %s -> skipping", id, err)
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
			log.Errorf("cassandra: could not close iterator for partition = %d: %s", partition, err.Error())
		}

		if len(defs) == 0 {
			return
		}

		// TODO - max batch size?
		cm := &schema.ControlMsg{
			Defs: defs,
			Op:   schema.OpRestore,
		}

		var b bytes.Buffer
		b.WriteByte(byte(msg.FormatIndexControlMessage))
		w := msgp.NewWriterSize(&b, 300)
		err := cm.EncodeMsg(w)
		if err != nil {
			log.Warnf("Failed to encode ControlMsg from partition = %d, err = %s", partition, err)
			return
		}
		w.Flush()

		log.Infof("Restoring %d series from partition %d", len(defs), partition)

		_, _, err = producer.producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(b.Bytes()),
			Partition: int32(partition),
		})
		if err != nil {
			log.Warnf("Failed to send ControlMsg for partition = %d, err = %s", partition, err)
			return
		}
	}

	// For large archives, this could take quite a long time. Process asynchronously and return response to know the job wsa kicked off.
	// TODO - provide an identifier and log progress?
	go func() {
		// TODO - make concurrent (configurable concurrency)
		for p := 0; p < numPartitions; p++ {
			scanPartition(p)
		}
	}()

	retval := controlmodels.IndexRestoreResp{}
	response.Write(ctx, response.NewJson(200, retval, ""))
}
