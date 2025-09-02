package memtable

import "project/blockmanager"

type BTreeNode struct {
	records   []*blockmanager.Record
	children  []*BTreeNode
	parent    *BTreeNode
	isLeaf    bool
	isDeleted []bool
}

func NewBTreeNode(parent *BTreeNode, isLeaf bool) *BTreeNode {
	return &BTreeNode{
		records:   []*blockmanager.Record{},
		children:  []*BTreeNode{},
		parent:    parent,
		isLeaf:    isLeaf,
		isDeleted: []bool{},
	}
}

func (n *BTreeNode) search(key string) *blockmanager.Record {
	i := 0
	// Trazenje kljuca u trenutnom cvoru
	for i < len(n.records) && key > n.records[i].GetKey() {
		i++
	}

	//Ako je kljuc pronadjen u trenutnom cvoru vrati record
	if i < len(n.records) && key == n.records[i].GetKey() && !n.isDeleted[i] {
		return n.records[i]
	}

	//Ako je cvor list, kljuc nije pronadjen
	if n.isLeaf {
		return nil
	}

	//Rekurzivno trazi kljuc u odgovarajucem podstablu
	return n.children[i].search(key)
}
