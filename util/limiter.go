package util

import "context"

// Limiter provides a mechanism for limiting concurrency.
// Users need to call Acquire() before starting work and Release()
// when the work is done.
// Acquire() will block if the Limiters limit has already been reached and
// unblock when another thread calls Release(), or the passed context is canceled.
type Limiter chan struct{}

// Acquire() will block if the limit is reached.  It will unblock
// when another thread calls Release(), or if the context is done.
// If we unblock due to the context being done, the return value will be
// "false".
func (l Limiter) Acquire(ctx context.Context) bool {

	// if the ctx is already canceled return straight away.
	select {
	case <-ctx.Done():
		return false
	default:
	}

	select {
	case <-ctx.Done():
		return false
	case l <- struct{}{}:
	}
	return true
}

func (l Limiter) Release() { <-l }

func NewLimiter(l int) Limiter {
	return make(chan struct{}, l)
}
