package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	managers map[string]*OffsetMgr
	mu       sync.Mutex
)

func init() {
	managers = make(map[string]*OffsetMgr)
}

type OffsetMgr struct {
	path string
	db   *leveldb.DB
	sync.Mutex
	users int
}

// Returns an OffsetMgr using a leveldb database in the passed directory.
// directory can be empty for working dir, or any relative or absolute path.
// If there is already a OffsetMgr open using the same dir, then it is
// returned instead of creating a new one.
func NewOffsetMgr(dir string) (*OffsetMgr, error) {

	// note that dir can be anything like '' (working dir), ., .., ./., ./.. etc
	dbFile := filepath.Join(dir, "partitionOffsets.db")

	//make sure the needed directory exists.
	err := os.MkdirAll(filepath.Base(dbFile), 0755)
	if err != nil {
		return nil, err
	}

	//check if this db is already opened.
	mu.Lock()
	defer mu.Unlock()
	if mgr, ok := managers[dbFile]; ok {
		mgr.Open()
		return mgr, nil
	}

	db, err := leveldb.OpenFile(dbFile, &opt.Options{})
	if err != nil {
		if _, ok := err.(*storage.ErrCorrupted); ok {
			log.Warn("partitionOffsets.db is corrupt. Recovering.")
			db, err = leveldb.RecoverFile(dbFile, &opt.Options{})
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	log.Info("Opened %s", dbFile)
	mgr := &OffsetMgr{
		path:  dbFile,
		db:    db,
		users: 1,
	}
	managers[dbFile] = mgr
	return mgr, nil
}

func (o *OffsetMgr) Open() {
	o.Lock()
	o.users++
	o.Unlock()
}

func (o *OffsetMgr) Close() {
	// aquire the package lock to prevent a possible deadlock if
	// NewOffsetMgr is called at the same time.
	mu.Lock()

	o.Lock()
	o.users--
	if o.users == 0 {
		log.Info("Closing partitionsOffset DB.")
		o.db.Close()

		// remove the mgr from the registry
		delete(managers, o.path)
	}
	o.Unlock()
	mu.Unlock()
}

func (o *OffsetMgr) Commit(topic string, partition int32, offset int64) error {
	key := new(bytes.Buffer)
	key.WriteString(fmt.Sprintf("T:%s-P:%d", topic, partition))
	data := new(bytes.Buffer)
	if err := binary.Write(data, binary.LittleEndian, offset); err != nil {
		return err
	}
	log.Debug("commiting offset %d for %s:%d to partitionsOffset.db", offset, topic, partition)
	return o.db.Put(key.Bytes(), data.Bytes(), &opt.WriteOptions{Sync: true})
}

func (o *OffsetMgr) Last(topic string, partition int32) (int64, error) {
	key := new(bytes.Buffer)
	key.WriteString(fmt.Sprintf("T:%s-P:%d", topic, partition))
	data, err := o.db.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Debug("no offset recorded for %s:%d", topic, partition)
			return -1, nil
		}
		return 0, err
	}
	var offset int64
	err = binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &offset)
	if err != nil {
		return 0, err
	}
	log.Debug("found saved offset %d for %s:%d", offset, topic, partition)
	return offset, nil
}
