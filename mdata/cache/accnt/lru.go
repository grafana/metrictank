package accnt

import (
	"container/list"
)

type LRU struct {
	list  *list.List
	items map[interface{}]*list.Element
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
