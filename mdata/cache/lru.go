package cache

import (
	"container/list"
	"sync"
)

type entry struct {
	v interface{}
}

type LRU struct {
	sync.Mutex
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
	l.Lock()
	defer l.Unlock()
	if ent, ok := l.items[key]; ok {
		l.list.MoveToFront(ent)
	} else {
		e := &entry{key}
		ent := l.list.PushFront(e)
		l.items[key] = ent
	}
}

func (l *LRU) pop() interface{} {
	l.Lock()
	defer l.Unlock()
	ent := l.list.Back()
	if ent != nil {
		l.list.Remove(ent)
		e := ent.Value.(*entry)
		delete(l.items, e.v)
		return e.v
	} else {
		return nil
	}
}
