package util

import (
	"sync"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// CassandraSession stores a connection to Cassandra along with associated configurations
type CassandraSession struct {
	session                 *gocql.Session
	cluster                 *gocql.ClusterConfig
	shutdown                chan struct{}
	connectionCheckTimeout  time.Duration
	connectionCheckInterval time.Duration
	addrs                   string
	logPrefix               string
	sync.RWMutex
}

// NewCassandraSession creates and returns a CassandraSession
func NewCassandraSession(session *gocql.Session,
	clusterConfig *gocql.ClusterConfig,
	shutdown chan struct{},
	timeout time.Duration,
	interval time.Duration,
	addrs string,
	logPrefix string) *CassandraSession {
	if clusterConfig == nil {
		panic("NewCassandraSession received nil pointer for ClusterConfig")
	}
	if session == nil {
		panic("NewCassandraSession received nil pointer for session")
	}

	cs := &CassandraSession{
		session:                 session,
		cluster:                 clusterConfig,
		shutdown:                shutdown,
		connectionCheckTimeout:  timeout,
		connectionCheckInterval: interval,
		addrs:                   addrs,
		logPrefix:               logPrefix,
	}

	return cs

}

// DeadConnectionCheck will run a query using the current Cassandra session every connectionCheckInterval
// if it cannot query Cassandra for longer than connectionCheckTimeout it will create a new session
//
// if you are not using a WaitGroup in the caller, just pass in nil
func (c *CassandraSession) DeadConnectionCheck(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ticker := time.NewTicker(time.Second * c.connectionCheckInterval)
	var totaltime time.Duration
	var err error
	var oldSession *gocql.Session

OUTER:
	for {
		// connection to cassandra has been down for longer than the configured timeout
		if totaltime >= c.connectionCheckTimeout {
			c.Lock()
			for {
				select {
				case <-c.shutdown:
					log.Infof("%s: received shutdown, exiting deadConnectionCheck", c.logPrefix)
					if c.session != nil && !c.session.Closed() {
						c.session.Close()
					}
					// make sure we unlock the sessionLock before returning
					c.Unlock()
					return
				default:
					log.Errorf("%s: creating new session to cassandra using hosts: %v", c.logPrefix, c.addrs)
					if c.session != nil && !c.session.Closed() && oldSession == nil {
						oldSession = c.session
					}
					c.session, err = c.cluster.CreateSession()
					if err != nil {
						log.Errorf("%s: error while attempting to recreate cassandra session. will retry after %v: %v", c.logPrefix, c.connectionCheckInterval.String(), err)
						time.Sleep(c.connectionCheckInterval)
						totaltime += c.connectionCheckInterval
						// continue inner loop to attempt to reconnect
						continue
					}
					c.Unlock()
					log.Errorf("%s: reconnecting to cassandra took %v", c.logPrefix, (totaltime - c.connectionCheckTimeout).String())
					totaltime = 0
					if oldSession != nil {
						oldSession.Close()
						oldSession = nil
					}
					// we connected, so go back to the normal outer loop
					continue OUTER
				}
			}
		}

		select {
		case <-c.shutdown:
			log.Infof("%s: received shutdown, exiting deadConnectionCheck", c.logPrefix)
			if c.session != nil && !c.session.Closed() {
				c.session.Close()
			}
			return
		case <-ticker.C:
			c.RLock()
			// this query should work on all cassandra deployments, but we may need to revisit this
			err = c.session.Query("SELECT cql_version FROM system.local").Exec()
			if err == nil {
				totaltime = 0
			} else {
				totaltime += c.connectionCheckInterval
				log.Errorf("%s: could not execute connection check query for %v: %v", c.logPrefix, totaltime.String(), err)
			}
			c.RUnlock()
		}
	}
}

// CurrentSession retrieves the current active cassandra session
//
// If the connection to Cassandra is down, this will block until it can be restored
func (c *CassandraSession) CurrentSession() *gocql.Session {
	c.RLock()
	session := c.session
	c.RUnlock()
	return session
}
