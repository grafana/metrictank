package archives

type key struct {
	schemaID    uint16
	rawInterval uint32
}

// Index tracks all seen lists of archives
type Index struct {
	seen map[key]int
	list []Archives
}

func NewIndex() Index {
	return Index{
		seen: make(map[key]int),
	}
}

// ArchivesID returns the archivesId for the given request,
// and adds it to the index if it is new
func (a *Index) ArchivesID(rawInterval uint32, schemaID uint16) int {
	key := key{
		schemaID:    schemaID,
		rawInterval: rawInterval,
	}
	idx, ok := a.seen[key]
	if !ok {
		idx = len(a.list)
		a.seen[key] = idx
		a.list = append(a.list, NewArchives(rawInterval, schemaID))
	}
	return idx
}

func (a Index) Get(index int) Archives {
	return a.list[index]
}
