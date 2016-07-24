package metricdef

import (
	"encoding/json"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"gopkg.in/raintank/schema.v1"
)

type DefsLevelDB struct {
	db *leveldb.DB
}

func NewDefsLevelDB(file string) (*DefsLevelDB, error) {
	//TODO:  handle bootstraping new nodes with a DB snapshot from a peer.

	//TODO: research to find the options we should be setting here.
	options := &opt.Options{}

	db, err := leveldb.OpenFile(file, options)
	if err != nil {
		return nil, err
	}
	log.Debug("initialized leveldb using file %s", file)
	return &DefsLevelDB{
		db: db,
	}, nil
}

func (d *DefsLevelDB) Stop() {
	d.db.Close()
}

func (d *DefsLevelDB) GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error) {
	metrics := make([]*schema.MetricDefinition, 0)
	iter := d.db.NewIterator(nil, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		metric, err := schema.MetricDefinitionFromJSON(iter.Value())
		if err != nil {
			log.Error(3, "failed to marshal metricDefinition. %s", err)
			continue
		}
		metrics = append(metrics, metric)
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, "", err
	}
	log.Debug("%d metricDefinitions fetched from leveldb.", len(metrics))
	return metrics, "", nil
}

func (d *DefsLevelDB) GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error) {
	data, err := d.db.Get([]byte(id), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	metric, err := schema.MetricDefinitionFromJSON(data)
	if err != nil {
		return nil, false, err
	}
	return metric, true, nil

}

func (d *DefsLevelDB) IndexMetric(m *schema.MetricDefinition) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	err = d.db.Put([]byte(m.Id), data, nil)
	log.Debug("metricDef written to leveldb.")
	return err
}

func (d *DefsLevelDB) SetAsyncResultCallback(fn ResultCallback) {
	//NOOP
	return
}
