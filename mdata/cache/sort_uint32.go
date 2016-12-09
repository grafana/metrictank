package cache

type sortableUint32 []uint32

func (su sortableUint32) Len() int           { return len(su) }
func (su sortableUint32) Swap(i, j int)      { su[i], su[j] = su[j], su[i] }
func (su sortableUint32) Less(i, j int) bool { return su[i] < su[j] }
