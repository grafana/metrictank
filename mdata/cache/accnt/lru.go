package accnt

import (
	"container/list"
)

type LRU struct {
	list  *list.List                    // the actual queue in which we move items around to represent their used time
	items map[interface{}]*list.Element // to find entries within the LRU so we can move them to the front
}

func NewLRU() *LRU {
	return &LRU{
		list:  list.New(),
		items: make(map[interface{}]*list.Element),
	}
}

func (l *LRU) touch(key interface{}) {
	if ent, ok := l.items[key]; ok {
		l.list.MoveToFront(ent)
	} else {
		l.items[key] = l.list.PushFront(key)
	}
}

func (l *LRU) del(key interface{}) {
	if ent, ok := l.items[key]; ok {
		l.list.Remove(ent)
		delete(l.items, key)
	}
}

func (l *LRU) pop() interface{} {
	ent := l.list.Back()
	if ent == nil {
		return nil
	}

	l.list.Remove(ent)
	e := ent.Value
	delete(l.items, e)
	return e
}

func (l *LRU) reset() {
	for {
		if l.pop() == nil {
			break
		}
	}
}
