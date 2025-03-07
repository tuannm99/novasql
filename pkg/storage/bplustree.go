package storage

import (
	"bytes"
)

type BPlusTree struct {
	root  *Node
	order int
}

type Node struct {
	isLeaf   bool
	keys     [][]byte
	values   [][]byte
	children []*Node
	parent   *Node
}

func (bpt *BPlusTree) Insert(key, value []byte) {
	if bpt.root == nil {
		bpt.root = &Node{isLeaf: true, keys: [][]byte{key}, values: [][]byte{value}}
		return
	}

	leaf := bpt.findLeafNode(key)
	bpt.insertIntoLeaf(leaf, key, value)

	if len(leaf.keys) > bpt.order {
		bpt.splitLeafNode(leaf)
	}
}

func (bpt *BPlusTree) findLeafNode(key []byte) *Node {
	node := bpt.root
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && bytes.Compare(key, node.keys[i]) > 0 {
			i++
		}
		node = node.children[i]
	}
	return node
}

func (bpt *BPlusTree) insertIntoLeaf(leaf *Node, key, value []byte) {
	i := 0
	for i < len(leaf.keys) && bytes.Compare(key, leaf.keys[i]) > 0 {
		i++
	}

	leaf.keys = append(leaf.keys[:i], append([][]byte{key}, leaf.keys[i:]...)...)
	leaf.values = append(leaf.values[:i], append([][]byte{value}, leaf.values[i:]...)...)
}

func (bpt *BPlusTree) splitLeafNode(leaf *Node) {
	mid := len(leaf.keys) / 2
	newLeaf := &Node{
		isLeaf: true,
		keys:   append([][]byte{}, leaf.keys[mid:]...),
		values: append([][]byte{}, leaf.values[mid:]...),
	}
	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]

	if leaf.parent == nil {
		newRoot := &Node{
			isLeaf:   false,
			keys:     [][]byte{newLeaf.keys[0]},
			children: []*Node{leaf, newLeaf},
		}
		leaf.parent = newRoot
		newLeaf.parent = newRoot
		bpt.root = newRoot
	} else {
		bpt.insertIntoParent(leaf, newLeaf.keys[0], newLeaf)
	}
}

func (bpt *BPlusTree) insertIntoParent(left *Node, key []byte, right *Node) {
	parent := left.parent
	i := 0
	for i < len(parent.keys) && bytes.Compare(key, parent.keys[i]) > 0 {
		i++
	}

	parent.keys = append(parent.keys[:i], append([][]byte{key}, parent.keys[i:]...)...)
	parent.children = append(parent.children[:i+1], append([]*Node{right}, parent.children[i+1:]...)...)

	if len(parent.keys) > bpt.order {
		bpt.splitInternalNode(parent)
	}
}

func (bpt *BPlusTree) splitInternalNode(node *Node) {
	mid := len(node.keys) / 2
	newInternal := &Node{
		isLeaf:   false,
		keys:     append([][]byte{}, node.keys[mid+1:]...),
		children: append([]*Node{}, node.children[mid+1:]...),
	}
	for _, child := range newInternal.children {
		child.parent = newInternal
	}
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]

	if node.parent == nil {
		newRoot := &Node{isLeaf: false, keys: [][]byte{node.keys[mid]}, children: []*Node{node, newInternal}}
		node.parent = newRoot
		newInternal.parent = newRoot
		bpt.root = newRoot
	} else {
		bpt.insertIntoParent(node, node.keys[mid], newInternal)
	}
}

func (bpt *BPlusTree) Delete(key []byte) {
	leaf := bpt.findLeafNode(key)
	bpt.deleteFromLeaf(leaf, key)

	if leaf.parent != nil && len(leaf.keys) < bpt.order/2 {
		bpt.rebalanceAfterDelete(leaf)
	}
}

func (bpt *BPlusTree) deleteFromLeaf(leaf *Node, key []byte) {
	i := 0
	for i < len(leaf.keys) && bytes.Compare(key, leaf.keys[i]) != 0 {
		i++
	}
	if i == len(leaf.keys) {
		return // Key not found
	}

	leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
	leaf.values = append(leaf.values[:i], leaf.values[i+1:]...)
}

func (bpt *BPlusTree) rebalanceAfterDelete(node *Node) {
	parent := node.parent
	i := 0
	for i < len(parent.children) && parent.children[i] != node {
		i++
	}

	leftSibling := i - 1
	rightSibling := i + 1

	if leftSibling >= 0 && len(parent.children[leftSibling].keys) > bpt.order/2 {
		node.keys = append([][]byte{parent.keys[leftSibling]}, node.keys...)
		parent.keys[leftSibling] = parent.children[leftSibling].keys[len(parent.children[leftSibling].keys)-1]
		parent.children[leftSibling].keys = parent.children[leftSibling].keys[:len(parent.children[leftSibling].keys)-1]
		return
	}

	if rightSibling < len(parent.children) &&
		len(parent.children[rightSibling].keys) > bpt.order/2 {
		node.keys = append(node.keys, parent.keys[i])
		parent.keys[i] = parent.children[rightSibling].keys[0]
		parent.children[rightSibling].keys = parent.children[rightSibling].keys[1:]
		return
	}

	if leftSibling >= 0 {
		parent.children[leftSibling].keys = append(parent.children[leftSibling].keys, parent.keys[leftSibling])
		parent.children[leftSibling].keys = append(parent.children[leftSibling].keys, node.keys...)
		parent.keys = append(parent.keys[:leftSibling], parent.keys[leftSibling+1:]...)
		parent.children = append(parent.children[:i], parent.children[i+1:]...)
	} else {
		node.keys = append(node.keys, parent.keys[i])
		node.keys = append(node.keys, parent.children[rightSibling].keys...)
		parent.keys = append(parent.keys[:i], parent.keys[i+1:]...)
		parent.children = append(parent.children[:rightSibling], parent.children[rightSibling+1:]...)
	}

	if parent.parent == nil && len(parent.keys) == 0 {
		bpt.root = parent.children[0]
		bpt.root.parent = nil
	}
}
