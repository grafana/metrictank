package cache

/*** Warning: None of this is thread safe ***/

type Accounting struct {
	total   uint64
	metrics map[string]*MetricAccounting
}

type MetricAccounting struct {
	total  uint64
	chunks map[uint32]uint64
}

func NewAccounting() *Accounting {
	return &Accounting{
		total:   0,
		metrics: make(map[string]*MetricAccounting),
	}
}

func (a *Accounting) GetTotal() uint64 {
	return a.total
}

func (a *Accounting) Add(metric string, ts uint32, size uint64) bool {
	var met *MetricAccounting
	var ok bool

	if met, ok = a.metrics[metric]; !ok {
		met = &MetricAccounting{
			total:  0,
			chunks: make(map[uint32]uint64),
		}
		a.metrics[metric] = met
	}

	if _, ok = met.chunks[ts]; ok {
		// we already have that chunk
		return false
	}

	met.chunks[ts] = size
	met.total = met.total + size
	a.total = a.total + size

	return true
}

func (a *Accounting) Del(metric string, ts uint32) bool {
	var met *MetricAccounting
	var ok bool
	var size uint64

	if met, ok = a.metrics[metric]; !ok {
		// we don't have this metric
		return false
	}

	if size, ok = met.chunks[ts]; !ok {
		// we don't have that chunk
		return false
	}

	delete(met.chunks, ts)
	met.total = met.total - size
	if met.total == 0 {
		delete(a.metrics, metric)
	}

	a.total = a.total - size

	return true
}
