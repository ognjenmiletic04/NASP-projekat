package cache

import (
	"container/list"
	"project/blockmanager"
)

type entry struct {
	key   string
	value *blockmanager.Record
}

type Cache struct {
	list     *list.List
	table    map[string]*list.Element
	capacity int
}

func NewCache(capacity int) *Cache {
	return &Cache{
		capacity: capacity,
		table:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (c *Cache) Put(record *blockmanager.Record) {
	elem, ok := c.table[record.GetKey()]
	if ok {
		elem.Value.(*entry).value = record
		c.list.MoveToBack(elem)
	} else {
		e := &entry{record.GetKey(), record}
		elem := c.list.PushBack(e)
		c.table[record.GetKey()] = elem
		if c.list.Len() > c.capacity {
			el := c.list.Front()
			c.list.Remove(el)
			delete(c.table, el.Value.(*entry).key)
		}
	}
}

func (c *Cache) Get(key string) (*blockmanager.Record, bool) {
	elem, ok := c.table[key]
	if ok {
		c.list.MoveToBack(elem)
		return elem.Value.(*entry).value, true
	}
	return nil, false
}
