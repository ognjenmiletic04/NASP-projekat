package sstable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"project/blockmanager"
)

type TreeNode struct {
	hashValue   []byte
	parent      *TreeNode
	left, right *TreeNode
	block       *blockmanager.Block
}

type MerkleTree struct {
	root *TreeNode
}

func newLeaf(block *blockmanager.Block) *TreeNode {
	data := block.ToBytes()
	hash := sha256.Sum256(data)
	return &TreeNode{
		hashValue: hash[:],
		block:     block,
	}
}

func newParent(left, right *TreeNode) *TreeNode {
	combined := append(left.hashValue, right.hashValue...)
	hash := sha256.Sum256(combined)
	parent := &TreeNode{
		hashValue: hash[:],
		left:      left,
		right:     right,
	}
	left.parent = parent
	right.parent = parent
	return parent
}

func CreateMerkleTree(blocks []*blockmanager.Block) *MerkleTree {
	children := make([]*TreeNode, 0)
	for _, block := range blocks {
		children = append(children, newLeaf(block))
	}
	for {
		if len(children)%2 == 1 {
			emptyHash := sha256.Sum256([]byte{})
			children = append(children, &TreeNode{hashValue: emptyHash[:]})
		}
		parents := make([]*TreeNode, 0)
		for i := 0; i < len(children); i += 2 {
			parent := newParent(children[i], children[i+1])
			parents = append(parents, parent)
		}
		children = parents
		if len(children) == 1 {
			break
		}
	}
	return &MerkleTree{
		root: children[0],
	}
}

func (mt *MerkleTree) IsValid(blocks []*blockmanager.Block) (bool, *blockmanager.Block) {
	other := CreateMerkleTree(blocks)
	if bytes.Equal(mt.root.hashValue, other.root.hashValue) {
		return true, nil
	}
	changedBlock := findChanged(mt.root, other.root)
	return false, changedBlock

}

func findChanged(oldNode, newNode *TreeNode) *blockmanager.Block {
	if bytes.Equal(oldNode.hashValue, newNode.hashValue) {
		return nil
	}
	if oldNode.left == nil && oldNode.right == nil {
		return oldNode.block
	}
	if oldNode.left != nil && newNode.left != nil {
		if changed := findChanged(oldNode.left, newNode.left); changed != nil {
			return changed
		}
	}
	if oldNode.right != nil && newNode.right != nil {
		if changed := findChanged(oldNode.right, newNode.right); changed != nil {
			return changed
		}
	}
	return nil
}

func (mt *MerkleTree) Serialize(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var writeNode func(node *TreeNode)
	writeNode = func(node *TreeNode) {
		if node == nil {
			return
		}
		if node.block != nil {
			file.Write([]byte{1})
		} else {
			file.Write([]byte{0})
		}
		hashLen := uint16(len(node.hashValue))
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, hashLen)
		file.Write(b)
		file.Write(node.hashValue)
		if node.block != nil {
			blockNum := node.block.GetBlockNumber()
			blockNumBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(blockNumBytes, blockNum)
			file.Write(blockNumBytes)

			pathBytes := []byte(node.block.GetBlockFilePath())
			pathLen := uint16(len(pathBytes))
			binary.LittleEndian.PutUint16(b, pathLen)
			file.Write(b)
			file.Write(pathBytes)
		}
		writeNode(node.left)
		writeNode(node.right)
	}
	writeNode(mt.root)
}

func (mt *MerkleTree) Deserialize(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var readNode func() *TreeNode
	readNode = func() *TreeNode {
		flag := make([]byte, 1)
		_, err := file.Read(flag)
		if err != nil {
			return nil
		}

		hashLenBytes := make([]byte, 2)
		file.Read(hashLenBytes)
		hashLen := binary.LittleEndian.Uint16(hashLenBytes)

		hash := make([]byte, hashLen)
		file.Read(hash)

		node := &TreeNode{hashValue: hash}

		if flag[0] == 1 { // leaf
			blockNumBytes := make([]byte, 8)
			file.Read(blockNumBytes)
			blockNum := binary.LittleEndian.Uint64(blockNumBytes)

			pathLenBytes := make([]byte, 2)
			file.Read(pathLenBytes)
			pathLen := binary.LittleEndian.Uint16(pathLenBytes)

			pathBytes := make([]byte, pathLen)
			file.Read(pathBytes)

			block := &blockmanager.Block{}
			block.SetBlockNumber(blockNum)
			block.SetBlockFilePath(string(pathBytes))
			node.block = block
		}
		node.left = readNode()
		node.right = readNode()
		if node.left != nil {
			node.left.parent = node
		}
		if node.right != nil {
			node.right.parent = node
		}
		return node
	}
	mt.root = readNode()
}
