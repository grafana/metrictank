package main

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
)

type PrintNotifierHandler struct{}

func NewPrintNotifierHandler() PrintNotifierHandler {
	return PrintNotifierHandler{}
}

func (dn PrintNotifierHandler) PartitionOf(key schema.MKey) (int32, bool) {
	return 0, false
}

func (dn PrintNotifierHandler) Handle(data []byte) {
	version := uint8(data[0])
	if version == uint8(mdata.PersistMessageBatchV1) {
		batch := mdata.PersistMessageBatch{}
		err := json.Unmarshal(data[1:], &batch)
		if err != nil {
			log.Errorf("failed to unmarsh batch message: %s -- skipping", err)
			return
		}
		for _, c := range batch.SavedChunks {
			fmt.Printf("%s %d %s\n", batch.Instance, c.T0, c.Key)
		}
	} else {
		log.Errorf("unknown message version %d", version)
	}
	return
}
