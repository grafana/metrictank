package memory

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// PriorityRWMutex is a mutex that allows fair access to high priority read operations while
// blocking low priority operations if a write lock is waiting.
type PriorityRWMutex struct {
	lock        sync.RWMutex
	writeWaiter sync.WaitGroup
	readWaiter  sync.WaitGroup
}

type BlockContext struct {
	lock         *PriorityRWMutex
	preLockTime  time.Time
	postLockTime time.Time
	postOpTime   time.Time
}

// RLockHigh allows *guaranteed fast* (or very high priority) ops to acquire the read lock as
// quickly as possible. Note: This can block write lock acquisition
func (pm *PriorityRWMutex) RLockHigh() {
	// Don't increment activeReads just grab the lock. If an active write is holding the lock it
	// will block, otherwise it *may* block a write acquisition, but that is opted into.
	pm.lock.RLock()
}

// RUnlockHigh unlocks read lock called via RLockHigh
func (pm *PriorityRWMutex) RUnlockHigh() {
	pm.lock.RUnlock()
}

// Synonym for RLockLow
func (pm *PriorityRWMutex) RLock() BlockContext {
	return pm.RLockLow()
}

// Synonym for RUnlockLow
func (pm *PriorityRWMutex) RUnlock(bc *BlockContext) {
	pm.RUnlockLow(bc)
}

// RLockLow will block until requested writes complete. New writes that come in also get priority.
func (pm *PriorityRWMutex) RLockLow() BlockContext {
	// Wait for any pending writes
	bc := BlockContext{lock: pm, preLockTime: time.Now()}
	pm.writeWaiter.Wait()
	pm.lock.RLock()
	bc.postLockTime = time.Now()
	pm.readWaiter.Add(1)
	return bc
}

// RUnlockLow unlocks read lock called via RLockLow
func (pm *PriorityRWMutex) RUnlockLow(bc *BlockContext) {
	pm.readWaiter.Done()
	bc.postOpTime = time.Now()
	pm.lock.RUnlock()
}

// Lock will block new low prio read locks until can acquire the lock. High prio read locks will
// not be blocked or accounted for.
func (pm *PriorityRWMutex) Lock() BlockContext {
	bc := BlockContext{lock: pm, preLockTime: time.Now()}
	pm.writeWaiter.Add(1)
	// Wait for any active reads
	pm.readWaiter.Wait()
	pm.lock.Lock()
	bc.postLockTime = time.Now()
	return bc
}

// Unlock unlocks mutex acquired via Lock
func (pm *PriorityRWMutex) Unlock(bc *BlockContext) {
	pm.writeWaiter.Done()
	bc.postOpTime = time.Now()
	pm.lock.Unlock()
}

func (bc *BlockContext) RUnlockLow(op string, logCb func() interface{}) {
	bc.lock.RUnlockLow(bc)

	lockWaitTime := bc.postLockTime.Sub(bc.preLockTime)
	lockHoldTime := bc.postOpTime.Sub(bc.postLockTime)

	if lockHoldTime > 5*time.Second {
		bc.logLongOp("Read", op, getDetails(logCb))
	} else if lockWaitTime > time.Second && lockWaitTime > 5*lockHoldTime {
		bc.logLongOp("Blocked Read", op, getDetails(logCb))
	}
}

func (bc *BlockContext) Unlock(op string, logCb func() interface{}) {
	bc.lock.Unlock(bc)

	lockWaitTime := bc.postLockTime.Sub(bc.preLockTime)
	lockHoldTime := bc.postOpTime.Sub(bc.postLockTime)

	if lockHoldTime > 100*time.Millisecond {
		bc.logLongOp("Write", op, getDetails(logCb))
	} else if lockWaitTime > 5*time.Second {
		bc.logLongOp("Blocked Write", op, getDetails(logCb))
	}
}

func (bc *BlockContext) logLongOp(opType, op, details string) {
	waitSecs := bc.postLockTime.Sub(bc.preLockTime).Seconds()
	holdsSecs := bc.postOpTime.Sub(bc.postLockTime).Seconds()

	timeFormat := "15:04:05.000"

	log.Infof("Long %s %s: lockWaitTime = %f, lockHoldTime = %f %s absoluteTimes = (%v -> %v -> %v)",
		opType, op, waitSecs, holdsSecs, details,
		bc.preLockTime.Format(timeFormat), bc.postLockTime.Format(timeFormat), bc.postOpTime.Format(timeFormat))
}

func getDetails(logCb func() interface{}) string {
	if logCb == nil {
		return ""
	}
	return fmt.Sprintf(", details = '%v'", logCb())
}

/*
type timedRLocker struct {
	lock              *sync.RWMutex
	preLock, postLock time.Time
}

func acquireTimedLock(lock *sync.RWMutex) timedRLocker {
	trl := timedRLocker{
		lock:    lock,
		preLock: time.Now(),
	}
	trl.lock.RLock()
	trl.postLock = time.Now()
	return trl
}

func (trl timedRLocker) RUnlock(op string, logCb func() interface{}) {
	endTime := time.Now()
	trl.lock.RUnlock()

	lockHoldTime := endTime.Sub(trl.postLock)
	lockWaitTime := trl.postLock.Sub(trl.preLock)
	if lockHoldTime > time.Second {
		log.Infof("Long %s: lockWaitTime = %f, lockHoldTime = %f (%v - %v), details = '%v' ", op, lockWaitTime.Seconds(), lockHoldTime.Seconds(), trl.postLock, endTime, logCb())
	} else if lockWaitTime > time.Second {
		log.Infof("Long Blocked %s: lockWaitTime = %f, lockHoldTime = %f, details = '%v'", op, lockWaitTime.Seconds(), lockHoldTime.Seconds(), logCb())
	}
}
*/
