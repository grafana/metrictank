package util

import "context"

// Limiter provides a mechanism for limiting concurrency.
// Users need to call Acquire() before starting work and Release()
// when the work is done.
// Acquire() will block if the Limiters limit has already been reached and
// unblock when another thread calls Release(), or the passed context is canceled.
type Limiter chan struct{}

// NewLimiter creates a limiter with l slots
func NewLimiter(l int) Limiter {
	return make(chan struct{}, l)
}

// Acquire returns when a slot is available.
// If the limit is reached it will block until
// another thread calls Release() or the context is done.
// In the latter case we return false.
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
