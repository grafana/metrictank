package metricname

import (
	"context"
	"math/rand"
	"sync/atomic"
)

type ReturnedNamesBuffer struct {
	nameGenerator     NameGenerator
	returnedValues    []atomic.Value
	lastReturnedValue uint32
}

func NewReturnedNamesBuffer(nameGenerator NameGenerator, bufferSize uint32) *ReturnedNamesBuffer {
	return &ReturnedNamesBuffer{
		nameGenerator:  nameGenerator,
		returnedValues: make([]atomic.Value, bufferSize),
	}
}

func (r *ReturnedNamesBuffer) Start(ctx context.Context, threadCount uint32) {
	r.nameGenerator.Start(ctx, threadCount)
}

func (r *ReturnedNamesBuffer) GetNewMetricName() string {
	returnValue := r.nameGenerator.GetNewMetricName()
	go func() {
		insertAt := rand.Intn(len(r.returnedValues))
		r.returnedValues[insertAt].Store(returnValue)
		atomic.StoreUint32(&r.lastReturnedValue, uint32(insertAt))
	}()
	return returnValue
}

func (r *ReturnedNamesBuffer) GetReturnedValue() string {
	loadAt := rand.Intn(len(r.returnedValues))
	returnValue := r.returnedValues[loadAt].Load().(string)
	if returnValue == "" {
		lastReturnedValue := atomic.LoadUint32(&r.lastReturnedValue)
		returnValue = r.returnedValues[lastReturnedValue].Load().(string)
	}
	return returnValue
}
