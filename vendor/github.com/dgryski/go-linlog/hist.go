package linlog

import "sync/atomic"

type Histogram struct {
	max    uint64
	linear uint64
	subbin uint64
	bins   []uint64
	sizes  []uint64
}

func NewHistogram(max, linear, subbin uint64) *Histogram {
	_, size := BinOf(max, linear, subbin)
	return &Histogram{
		max:    max,
		linear: linear,
		subbin: subbin,
		bins:   make([]uint64, size+1),
		sizes:  Bins(max, linear, subbin),
	}
}

func (h *Histogram) Insert(n uint64) {
	_, bin := BinOf(n, h.linear, h.subbin)
	if bin >= uint64(len(h.bins)) {
		bin = uint64(len(h.bins) - 1)
	}
	h.bins[bin]++
}

func (h *Histogram) AtomicInsert(n uint64) {
	_, bin := BinOf(n, h.linear, h.subbin)
	if bin >= uint64(len(h.bins)) {
		bin = uint64(len(h.bins) - 1)
	}
	atomic.AddUint64(&h.bins[bin], 1)
}

type Bin struct {
	Size  uint64
	Count uint64
}

func (h *Histogram) Bins() []Bin {
	bins := make([]Bin, len(h.bins))
	for i, v := range h.bins {
		bins[i] = Bin{h.sizes[i], v}
	}
	return bins
}

func (h *Histogram) AtomicBins() []Bin {
	bins := make([]Bin, len(h.bins))
	for i := range h.bins {
		bins[i] = Bin{h.sizes[i], atomic.LoadUint64(&h.bins[i])}
	}
	return bins
}
