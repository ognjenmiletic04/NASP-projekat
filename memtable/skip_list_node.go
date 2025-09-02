package memtable

import "project/blockmanager"

type Node struct {
	record *blockmanager.Record
	next   *Node
	below  *Node
	level  int
}

func NewNode(record *blockmanager.Record, level int, next *Node, below *Node) *Node {
	return &Node{
		record: record,
		level:  level,
		next:   next,
		below:  below,
	}
}
