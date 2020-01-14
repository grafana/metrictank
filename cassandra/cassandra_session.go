package cassandra

import (
	"sync"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// Session stores a connection to Cassandra along with associated configurations
type Session struct {
	session                 *gocql.Session
	cluster                 *gocql.ClusterConfig
	shutdown                chan struct{}
	connectionCheckTimeout  time.Duration
	connectionCheckInterval time.Duration
	addrs                   string
	logPrefix               string
	sync.RWMutex
}

// NewSession creates and returns a CassandraSession
func NewSession(session *gocql.Session,
	clusterConfig *gocql.ClusterConfig,
	shutdown chan struct{},
	timeout time.Duration,
	interval time.Duration,
	addrs string,
	logPrefix string) *Session {
	if clusterConfig == nil {
		panic("NewSession received nil pointer for ClusterConfig")
	}
	if session == nil {
		panic("NewSession received nil pointer for session")
	}

	cs := &Session{
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
func (s *Session) DeadConnectionCheck(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ticker := time.NewTicker(time.Second * s.connectionCheckInterval)
	var totaltime time.Duration
	var err error
	var oldSession *gocql.Session

OUTER:
	for {
		// connection to Cassandra has been down for longer than the configured timeout
		if totaltime >= s.connectionCheckTimeout {
			s.Lock()
			for {
				select {
				case <-s.shutdown:
					log.Infof("%s: received shutdown, exiting deadConnectionCheck", s.logPrefix)
					if s.session != nil && !s.session.Closed() {
						s.session.Close()
					}
					// make sure we unlock the sessionLock before returning
					s.Unlock()
					return
				default:
					log.Errorf("%s: creating new session to cassandra using hosts: %v", s.logPrefix, s.addrs)
					if s.session != nil && !s.session.Closed() && oldSession == nil {
						oldSession = s.session
					}
					s.session, err = s.cluster.CreateSession()
					if err != nil {
						log.Errorf("%s: error while attempting to recreate cassandra session. will retry after %v: %v", s.logPrefix, s.connectionCheckInterval.String(), err)
						time.Sleep(s.connectionCheckInterval)
						totaltime += s.connectionCheckInterval
						// continue inner loop to attempt to reconnect
						continue
					}
					s.Unlock()
					log.Errorf("%s: reconnecting to cassandra took %v", s.logPrefix, (totaltime - s.connectionCheckTimeout).String())
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
		case <-s.shutdown:
			log.Infof("%s: received shutdown, exiting deadConnectionCheck", s.logPrefix)
			if s.session != nil && !s.session.Closed() {
				s.session.Close()
			}
			return
		case <-ticker.C:
			s.RLock()
			// this query should work on all cassandra deployments, but we may need to revisit this
			err = s.session.Query("SELECT cql_version FROM system.local").Exec()
			if err == nil {
				totaltime = 0
			} else {
				totaltime += s.connectionCheckInterval
				log.Errorf("%s: could not execute connection check query for %v: %v", s.logPrefix, totaltime.String(), err)
			}
			s.RUnlock()
		}
	}
}

// CurrentSession retrieves the current active Cassandra session
//
// If the connection to Cassandra is down, this will block until it can be restored
func (s *Session) CurrentSession() *gocql.Session {
	s.RLock()
	session := s.session
	s.RUnlock()
	return session
}
