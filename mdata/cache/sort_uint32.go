package cache

type uint32Asc []uint32

func (su uint32Asc) Len() int           { return len(su) }
func (su uint32Asc) Swap(i, j int)      { su[i], su[j] = su[j], su[i] }
func (su uint32Asc) Less(i, j int) bool { return su[i] < su[j] }
