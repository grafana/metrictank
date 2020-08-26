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
	lowPrioLock sync.RWMutex
}

// BlockContext is used to track the stages of lock acquisition and release so that timing
// may be reported (and include salient details of the operation the lock was needed for)
type BlockContext struct {
	lock         *PriorityRWMutex
	preLockTime  time.Time
	postLockTime time.Time
	postOpTime   time.Time
}

// RLockHigh allows *guaranteed fast* (or very high priority) ops to acquire the read lock as
// quickly as possible. Note: This can block write lock acquisition
func (pm *PriorityRWMutex) RLockHigh() {
	// If an active write is holding the lock this call will block
	// otherwise, this call *may* block a write acquisition, but that is ok for "high" priority.
	pm.lock.RLock()
}

// RUnlockHigh unlocks read lock called via RLockHigh
func (pm *PriorityRWMutex) RUnlockHigh() {
	pm.lock.RUnlock()
}

// RLockLow will block until requested writes complete. New writes that come in also get priority.
func (pm *PriorityRWMutex) RLockLow() BlockContext {
	// Wait for any pending writes
	bc := BlockContext{lock: pm, preLockTime: time.Now()}
	pm.lowPrioLock.RLock()
	pm.lock.RLock()
	bc.postLockTime = time.Now()
	return bc
}

// RUnlockLow unlocks read lock called via RLockLow
// Note: This function should not be called directly, but rather should be called via the returned BlockContext
func (pm *PriorityRWMutex) RUnlockLow(bc *BlockContext) {
	bc.postOpTime = time.Now()
	pm.lock.RUnlock()
	pm.lowPrioLock.RUnlock()
}

// Lock will block new low priority read locks until it can acquire the lock.
// High priority reads will not be blocked until all active low priority operations are complete.
func (pm *PriorityRWMutex) Lock() BlockContext {
	bc := BlockContext{lock: pm, preLockTime: time.Now()}
	pm.lowPrioLock.Lock()
	pm.lock.Lock()
	bc.postLockTime = time.Now()
	return bc
}

// Unlock unlocks mutex acquired via Lock
// Note: This function should not be called directly, but rather should be called via the returned BlockContext
func (pm *PriorityRWMutex) Unlock(bc *BlockContext) {
	bc.postOpTime = time.Now()
	pm.lock.Unlock()
	pm.lowPrioLock.Unlock()
}

// RUnlockLow will unlock the associated lock and log if the lock was held for a long time
// or acquisition was blocked for a significant length of time.
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

// Unlock will unlock the associated lock and log if the lock was held for a long time
// or acquisition was blocked for a significant length of time.
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
