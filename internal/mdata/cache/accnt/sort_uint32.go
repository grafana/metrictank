package accnt

type Uint32Asc []uint32

func (su Uint32Asc) Len() int           { return len(su) }
func (su Uint32Asc) Swap(i, j int)      { su[i], su[j] = su[j], su[i] }
func (su Uint32Asc) Less(i, j int) bool { return su[i] < su[j] }
