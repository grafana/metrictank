package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// EnsureTableExists checks if the specified table exists or not. If it does not exist and the
// create-keyspace flag is true, then it will create it, if it doesn't exist and the create-keyspace
// flag is false then it will retry 5 times with a 5s sleep before each retry. If at the end of the
// 5 retries the table still doesn't exist it returns an error.
// If the table already exists it returns nil.
// session:         cassandra session
// createKeyspace:  whether non-existent tables shall be created
// keyspace:        name of the keyspace
// schema:          table schema as a string
// table:           name of the table in cassandra
func EnsureTableExists(session *gocql.Session, createKeyspace bool, keyspace, schema, table string) error {
	var err error
	var attempt int

	if createKeyspace {
		log.Infof("cass-ensure-table-exists: ensuring that table %s exists.", table)
		err = session.Query(schema).Exec()
		if err != nil {
			return fmt.Errorf("failed to initialize cassandra table: %s", err)
		}
	} else {
		var keyspaceMetadata *gocql.KeyspaceMetadata
		for attempt = 1; attempt <= 5; attempt++ {
			keyspaceMetadata, err = session.KeyspaceMetadata(keyspace)
			if err != nil {
				err = fmt.Errorf("cassandra keyspace %s not found", keyspace)
			} else {
				if _, ok := keyspaceMetadata.Tables[table]; !ok {
					err = fmt.Errorf("cassandra table %s not found", table)
				} else {
					break
				}
			}

			log.Warnf("cass-ensure-table-exists: attempt %d, retrying in 5s: %s", attempt, err)
			time.Sleep(5 * time.Second)
		}
	}

	if err != nil {
		err = fmt.Errorf("attempt %d: %s", attempt, err)
		log.Errorf("cass-ensure-table-exists: %s", err)
	}

	return err
}
