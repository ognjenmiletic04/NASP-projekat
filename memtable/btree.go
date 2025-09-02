package memtable

import (
	"fmt"
	"project/blockmanager"
)

type BTree struct {
	root       *BTreeNode
	childCount int
}

func NewBTree(childCount int) *BTree {
	return &BTree{
		root:       nil,
		childCount: childCount,
		//childCount predstavlja min broj dece za cvor koji nije leaf
		//2*childCount max broj dece za cvor koji nije leaf
		//Min broj recorda = childCount-1 osim za koren
		//Max broj recorda = 2*childCount -1
	}
}

func (b *BTree) Search(key string) *blockmanager.Record {
	if b.root == nil {
		return nil
	}
	return b.root.search(key)
}

func (b *BTree) Insert(record *blockmanager.Record) {
	if b.root == nil {
		b.root = NewBTreeNode(nil, true)
		b.basicInsert(b.root, record)
		return
	}

	insertNode := b.findInsertNode(b.root, record.GetKey())
	parentNode := insertNode.parent
	if parentNode != nil {
		parentNode.isLeaf = false
	}
	if len(insertNode.records) < 2*b.childCount-1 {
		b.basicInsert(insertNode, record)
	} else {
		parent := insertNode.parent
		if parent == nil {
			b.splitChild(insertNode)
			b.basicInsert(insertNode, record)
			return
		}
		index := 0
		for parent.children[index] != insertNode {
			index++
		}

		if index > 0 && len(parent.children[index-1].records) < 2*b.childCount-1 {
			b.rotationInsert(insertNode, record)
		} else if index < len(parent.children)-1 && len(parent.children[index+1].records) < 2*b.childCount-1 {
			b.rotationInsert(insertNode, record)
		} else {
			b.splitChild(insertNode)
			b.Insert(record)
		}
	}
}

func (b *BTree) findInsertNode(node *BTreeNode, key string) *BTreeNode {
	if node.isLeaf {
		return node
	}

	i := 0
	for i < len(node.records) && key > node.records[i].GetKey() {
		i++
	}
	return b.findInsertNode(node.children[i], key)
}

func (b *BTree) basicInsert(node *BTreeNode, record *blockmanager.Record) {
	i := len(node.records) - 1
	node.records = append(node.records, nil)
	node.isDeleted = append(node.isDeleted, false)

	for i >= 0 && record.GetKey() < node.records[i].GetKey() {
		node.records[i+1] = node.records[i]
		i--
	}
	node.records[i+1] = record
}

func (b *BTree) rotationInsert(node *BTreeNode, record *blockmanager.Record) {
	parent := node.parent
	if parent == nil {
		return
	}
	index := 0
	for parent.children[index] != node {
		index++
	}

	if index > 0 && len(parent.children[index-1].records) < 2*b.childCount-1 {
		leftSibling := parent.children[index-1]
		leftSibling.records = append(leftSibling.records, parent.records[index-1])

		parent.records[index-1] = node.records[0]

		node.records = node.records[1:]
		b.basicInsert(node, record)
		return
	}

	if index < len(parent.children)-1 && len(parent.children[index+1].records) < 2*b.childCount-1 {
		rightSibling := parent.children[index+1]
		rightSibling.records = append([]*blockmanager.Record{parent.records[index]}, rightSibling.records...)

		parent.records[index] = node.records[len(node.records)-1]

		node.records = node.records[:len(node.records)-1]
		b.basicInsert(node, record)
		return
	}
}

func (b *BTree) splitChild(insertNode *BTreeNode) {
	childCount := b.childCount

	if insertNode.parent == nil {
		newRoot := NewBTreeNode(nil, false)
		newRoot.children = append(newRoot.children, insertNode)
		insertNode.parent = newRoot
		b.root = newRoot
	}
	parent := insertNode.parent

	index := 0
	for parent.children[index] != insertNode {
		index++
	}

	newChild := NewBTreeNode(parent, insertNode.isLeaf)
	newChild.records = append(newChild.records, insertNode.records[childCount:]...)
	if !insertNode.isLeaf {
		newChild.children = append(newChild.children, insertNode.children[childCount:]...)
		for _, child := range newChild.children {
			child.parent = newChild
		}
	}

	parent.children = append(parent.children[:index+1], append([]*BTreeNode{newChild}, parent.children[index+1:]...)...)
	parent.records = append(parent.records[:index], append([]*blockmanager.Record{insertNode.records[childCount-1]}, parent.records[index:]...)...)

	insertNode.records = insertNode.records[:childCount-1]
	if !insertNode.isLeaf {
		insertNode.children = insertNode.children[:childCount]
	}

	if len(parent.records) == 2*b.childCount-1 {
		b.splitChild(parent)
	}
}

func (b *BTree) PrintTree() {
	if b.root == nil {
		println("Empty tree")
		return
	}
	b.printTreeRecursive(b.root)
}

func (b *BTree) printTreeRecursive(node *BTreeNode) {
	if node == nil {
		return
	}

	keys := make([]string, len(node.records))
	for i, record := range node.records {
		keys[i] = record.GetKey()
	}
	fmt.Println(keys)

	for _, child := range node.children {
		b.printTreeRecursive(child)
	}
}

func (b *BTree) LogicallyDelete(key string) {
	record := b.Search(key)
	if record == nil {
		print("Key not found")
		return
	}

	// Treba da nađem node koji sadrži record i označim kao obrisan
	node := b.findNodeContaining(b.root, key)
	if node != nil {
		for i := 0; i < len(node.records); i++ {
			if node.records[i].GetKey() == key {
				node.isDeleted[i] = true
				return
			}
		}
	}
}

func (b *BTree) findNodeContaining(node *BTreeNode, key string) *BTreeNode {
	if node == nil {
		return nil
	}

	for i, record := range node.records {
		if record.GetKey() == key && !node.isDeleted[i] {
			return node
		}
	}

	if !node.isLeaf {
		for _, child := range node.children {
			result := b.findNodeContaining(child, key)
			if result != nil {
				return result
			}
		}
	}

	return nil
}
