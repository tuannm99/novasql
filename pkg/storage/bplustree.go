package storage

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// BTreeNodeHeader stores metadata about a B+ tree node
type BTreeNodeHeader struct {
	IsLeaf     bool
	KeyCount   uint16
	NextPageID uint32 // For leaf node linkage
	PrevPageID uint32 // For bidirectional traversal
}

// KeyValuePair represents a key-value pair in a B+ tree node
type KeyValuePair struct {
	Key   []byte
	Value []byte
}

// BTreeNode represents a node in a B+ tree
type BTreeNode struct {
	pageID   uint32
	header   BTreeNodeHeader
	keys     [][]byte
	values   [][]byte
	children []uint32
	dirty    bool
	tree     *BPlusTree
}

// BPlusTree represents a B+ tree index
type BPlusTree struct {
	rootPageID     uint32
	order          int
	storageManager *StorageManager
	mu             sync.RWMutex // Concurrency control
}

// NewBPlusTree creates a new B+ tree with the specified order
func NewBPlusTree(order int, sm *StorageManager) (*BPlusTree, error) {
	tree := &BPlusTree{
		order:          order,
		storageManager: sm,
	}

	// Create root node
	rootNode, err := tree.createNode(true) // Start with a leaf node
	if err != nil {
		return nil, err
	}

	tree.rootPageID = rootNode.pageID
	return tree, nil
}

// LoadBPlusTree loads an existing B+ tree from storage
func LoadBPlusTree(rootPageID uint32, order int, sm *StorageManager) (*BPlusTree, error) {
	return &BPlusTree{
		rootPageID:     rootPageID,
		order:          order,
		storageManager: sm,
	}, nil
}

// createNode creates a new node and saves it to disk
func (bpt *BPlusTree) createNode(isLeaf bool) (*BTreeNode, error) {
	// Allocate a new page
	page, err := bpt.storageManager.AllocatePage(BTree)
	if err != nil {
		return nil, err
	}

	// Create node
	node := &BTreeNode{
		pageID: page.ID,
		header: BTreeNodeHeader{
			IsLeaf:     isLeaf,
			KeyCount:   0,
			NextPageID: 0,
			PrevPageID: 0,
		},
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]uint32, 0),
		dirty:    true,
		tree:     bpt,
	}

	// Save node to disk
	if err := bpt.saveNode(node); err != nil {
		return nil, err
	}

	return node, nil
}

// loadNode loads a node from disk
func (bpt *BPlusTree) loadNode(pageID uint32) (*BTreeNode, error) {
	page, err := bpt.storageManager.LoadPage(pageID)
	if err != nil {
		return nil, err
	}

	// Deserialize node from page
	node := &BTreeNode{
		pageID: pageID,
		tree:   bpt,
	}

	// First bytes contain header
	headerSize := binary.Size(BTreeNodeHeader{})
	headerData := page.Data[:headerSize]

	// Deserialize header
	headerBuf := bytes.NewReader(headerData)
	if err := binary.Read(headerBuf, binary.LittleEndian, &node.header); err != nil {
		return nil, NewStorageError(ErrCodePageCorrupted, "Failed to deserialize B+Tree node header", err)
	}

	// Read key count
	keyCount := int(node.header.KeyCount)

	// Read keys and values or children
	offset := headerSize

	// Read keys
	for i := 0; i < keyCount; i++ {
		// Read key length
		keyLenBytes := page.Data[offset : offset+4]
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)
		offset += 4

		// Read key
		key := make([]byte, keyLen)
		copy(key, page.Data[offset:offset+int(keyLen)])
		node.keys = append(node.keys, key)
		offset += int(keyLen)
	}

	if node.header.IsLeaf {
		// Read values for leaf node
		for i := 0; i < keyCount; i++ {
			// Read value length
			valLenBytes := page.Data[offset : offset+4]
			valLen := binary.LittleEndian.Uint32(valLenBytes)
			offset += 4

			// Read value
			value := make([]byte, valLen)
			copy(value, page.Data[offset:offset+int(valLen)])
			node.values = append(node.values, value)
			offset += int(valLen)
		}
	} else {
		// Read child pointers for internal node
		for i := 0; i <= keyCount; i++ {
			childIDBytes := page.Data[offset : offset+4]
			childID := binary.LittleEndian.Uint32(childIDBytes)
			node.children = append(node.children, childID)
			offset += 4
		}
	}

	return node, nil
}

// saveNode saves a node to disk
func (bpt *BPlusTree) saveNode(node *BTreeNode) error {
	if !node.dirty {
		return nil
	}

	// Get page from storage manager
	page, err := bpt.storageManager.LoadPage(node.pageID)
	if err != nil {
		return err
	}

	// Update node header
	node.header.KeyCount = uint16(len(node.keys))

	// Serialize header
	headerBuf := new(bytes.Buffer)
	if err := binary.Write(headerBuf, binary.LittleEndian, node.header); err != nil {
		return NewStorageError(ErrCodeStorageIO, "Failed to serialize B+Tree node header", err)
	}

	// Copy header to page
	headerBytes := headerBuf.Bytes()
	copy(page.Data, headerBytes)

	// Set offset after header
	offset := len(headerBytes)

	// Write keys
	for _, key := range node.keys {
		// Write key length
		binary.LittleEndian.PutUint32(page.Data[offset:offset+4], uint32(len(key)))
		offset += 4

		// Write key
		copy(page.Data[offset:], key)
		offset += len(key)
	}

	if node.header.IsLeaf {
		// Write values for leaf node
		for _, value := range node.values {
			// Write value length
			binary.LittleEndian.PutUint32(page.Data[offset:offset+4], uint32(len(value)))
			offset += 4

			// Write value
			copy(page.Data[offset:], value)
			offset += len(value)
		}
	} else {
		// Write child pointers for internal node
		for _, childID := range node.children {
			binary.LittleEndian.PutUint32(page.Data[offset:offset+4], childID)
			offset += 4
		}
	}

	// Mark page as dirty and save
	page.dirty = true
	if err := bpt.storageManager.SavePage(page); err != nil {
		return err
	}

	node.dirty = false
	return nil
}

// Insert adds a key-value pair to the tree
func (bpt *BPlusTree) Insert(key, value []byte) error {
	bpt.mu.Lock()
	defer bpt.mu.Unlock()

	// Load root node
	root, err := bpt.loadNode(bpt.rootPageID)
	if err != nil {
		return err
	}

	// Handle root split case
	if len(root.keys) >= bpt.order {
		// Create new root
		newRoot, err := bpt.createNode(false)
		if err != nil {
			return err
		}

		// Old root becomes first child of new root
		newRoot.children = append(newRoot.children, root.pageID)

		// Split the old root
		if err := bpt.splitChild(newRoot, 0, root); err != nil {
			return err
		}

		// Update tree root
		bpt.rootPageID = newRoot.pageID
		root = newRoot
	}

	return bpt.insertNonFull(root, key, value)
}

// insertNonFull inserts into a non-full node
func (bpt *BPlusTree) insertNonFull(node *BTreeNode, key, value []byte) error {
	i := len(node.keys) - 1

	// If node is a leaf, insert the key-value pair
	if node.header.IsLeaf {
		// Find position to insert
		for i >= 0 && bytes.Compare(key, node.keys[i]) < 0 {
			i--
		}
		i++

		// Insert key and value at position i
		node.keys = append(node.keys, nil)
		copy(node.keys[i+1:], node.keys[i:])
		node.keys[i] = key

		node.values = append(node.values, nil)
		copy(node.values[i+1:], node.values[i:])
		node.values[i] = value

		node.dirty = true
		return bpt.saveNode(node)
	}

	// Find the child which is going to contain this key
	for i >= 0 && bytes.Compare(key, node.keys[i]) < 0 {
		i--
	}
	i++

	// Load the appropriate child
	child, err := bpt.loadNode(node.children[i])
	if err != nil {
		return err
	}

	// If child is full, split it
	if len(child.keys) >= bpt.order {
		if err := bpt.splitChild(node, i, child); err != nil {
			return err
		}

		// After split, the new key is either in node or in the new child
		if bytes.Compare(key, node.keys[i]) > 0 {
			i++
		}

		// Reload child, which may have changed
		child, err = bpt.loadNode(node.children[i])
		if err != nil {
			return err
		}
	}

	return bpt.insertNonFull(child, key, value)
}

// splitChild splits a full child of a node
func (bpt *BPlusTree) splitChild(parentNode *BTreeNode, childIndex int, childNode *BTreeNode) error {
	// Create a new node to hold half of the keys/values
	newNode, err := bpt.createNode(childNode.header.IsLeaf)
	if err != nil {
		return err
	}

	// Calculate split point
	splitPoint := bpt.order / 2

	// Setup sibling pointers for leaf nodes
	if childNode.header.IsLeaf {
		newNode.header.NextPageID = childNode.header.NextPageID
		childNode.header.NextPageID = newNode.pageID
		newNode.header.PrevPageID = childNode.pageID

		// Update next node's prev pointer if it exists
		if newNode.header.NextPageID != 0 {
			nextNode, err := bpt.loadNode(newNode.header.NextPageID)
			if err != nil {
				return err
			}
			nextNode.header.PrevPageID = newNode.pageID
			nextNode.dirty = true
			if err := bpt.saveNode(nextNode); err != nil {
				return err
			}
		}
	}

	// Move keys and values to the new node
	if childNode.header.IsLeaf {
		// For leaf, copy right half of keys and values
		newNode.keys = append(newNode.keys, childNode.keys[splitPoint:]...)
		newNode.values = append(newNode.values, childNode.values[splitPoint:]...)

		// Truncate original node
		childNode.keys = childNode.keys[:splitPoint]
		childNode.values = childNode.values[:splitPoint]
	} else {
		// For internal node, move right half of keys and children
		newNode.keys = append(newNode.keys, childNode.keys[splitPoint+1:]...)
		newNode.children = append(newNode.children, childNode.children[splitPoint+1:]...)

		// Save middle key for parent
		middleKey := childNode.keys[splitPoint]

		// Truncate original node
		childNode.keys = childNode.keys[:splitPoint]
		childNode.children = childNode.children[:splitPoint+1]

		// Insert middle key and new child pointer in parent
		parentNode.keys = append(parentNode.keys, nil)
		copy(parentNode.keys[childIndex+1:], parentNode.keys[childIndex:])
		parentNode.keys[childIndex] = middleKey

		parentNode.children = append(parentNode.children, 0)
		copy(parentNode.children[childIndex+2:], parentNode.children[childIndex+1:])
		parentNode.children[childIndex+1] = newNode.pageID
	}

	// Mark nodes as dirty and save them
	childNode.dirty = true
	newNode.dirty = true
	parentNode.dirty = true

	if err := bpt.saveNode(childNode); err != nil {
		return err
	}
	if err := bpt.saveNode(newNode); err != nil {
		return err
	}
	return bpt.saveNode(parentNode)
}

// Search finds a value by key
func (bpt *BPlusTree) Search(key []byte) ([]byte, error) {
	bpt.mu.RLock()
	defer bpt.mu.RUnlock()

	// Start at root
	nodeID := bpt.rootPageID

	for {
		node, err := bpt.loadNode(nodeID)
		if err != nil {
			return nil, err
		}

		// Binary search for key in this node
		found, index := bpt.findKey(node, key)

		if node.header.IsLeaf {
			if found {
				return node.values[index], nil
			}
			return nil, NewStorageError(ErrCodeInvalidOperation, "Key not found", nil)
		}

		// Follow the appropriate child pointer
		if found {
			nodeID = node.children[index+1]
		} else {
			nodeID = node.children[index]
		}
	}
}

// findKey performs binary search for a key in a node
func (bpt *BPlusTree) findKey(node *BTreeNode, key []byte) (bool, int) {
	low, high := 0, len(node.keys)-1

	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(key, node.keys[mid])

		if cmp == 0 {
			return true, mid
		} else if cmp < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return false, low
}

// Delete removes a key-value pair from the tree
func (bpt *BPlusTree) Delete(key []byte) error {
	bpt.mu.Lock()
	defer bpt.mu.Unlock()

	// Load root node
	root, err := bpt.loadNode(bpt.rootPageID)
	if err != nil {
		return err
	}

	// Handle empty root case
	if len(root.keys) == 0 && !root.header.IsLeaf {
		// If root is empty and not a leaf, make its only child the new root
		newRootID := root.children[0]

		// Free the old root page
		if err := bpt.storageManager.FreePage(root.pageID, BTree); err != nil {
			return err
		}

		// Update tree root
		bpt.rootPageID = newRootID
		root, err = bpt.loadNode(newRootID)
		if err != nil {
			return err
		}
	}

	// Delete the key
	return bpt.deleteKey(root, key)
}

// deleteKey deletes a key from a subtree
func (bpt *BPlusTree) deleteKey(node *BTreeNode, key []byte) error {
	// Find position of key or where it would be
	found, keyIndex := bpt.findKey(node, key)

	if node.header.IsLeaf {
		if !found {
			return NewStorageError(ErrCodeInvalidOperation, "Key not found", nil)
		}

		// Remove the key and value
		node.keys = append(node.keys[:keyIndex], node.keys[keyIndex+1:]...)
		node.values = append(node.values[:keyIndex], node.values[keyIndex+1:]...)
		node.dirty = true

		return bpt.saveNode(node)
	}

	// Handle internal node
	var childNode *BTreeNode
	var err error

	if found {
		// If key is in this node, get the child that precedes it
		childNode, err = bpt.loadNode(node.children[keyIndex])
	} else {
		// If key is not in this node, get the child where the key should be
		childNode, err = bpt.loadNode(node.children[keyIndex])
	}

	if err != nil {
		return err
	}

	// Check if child has enough keys
	minKeys := bpt.order / 2
	if len(childNode.keys) <= minKeys {
		return bpt.deleteHandleUnderflow(node, childNode, keyIndex)
	}

	// Recursively delete from child
	return bpt.deleteKey(childNode, key)
}

// deleteHandleUnderflow handles the case when a child has too few keys
func (bpt *BPlusTree) deleteHandleUnderflow(parentNode *BTreeNode, childNode *BTreeNode, childIndex int) error {
	minKeys := bpt.order / 2

	// Try borrowing from left sibling
	if childIndex > 0 {
		leftSibling, err := bpt.loadNode(parentNode.children[childIndex-1])
		if err != nil {
			return err
		}

		if len(leftSibling.keys) > minKeys {
			// Borrow from left sibling
			return bpt.borrowFromLeft(parentNode, childNode, leftSibling, childIndex)
		}
	}

	// Try borrowing from right sibling
	if childIndex < len(parentNode.children)-1 {
		rightSibling, err := bpt.loadNode(parentNode.children[childIndex+1])
		if err != nil {
			return err
		}

		if len(rightSibling.keys) > minKeys {
			// Borrow from right sibling
			return bpt.borrowFromRight(parentNode, childNode, rightSibling, childIndex)
		}
	}

	// Merge with a sibling
	if childIndex > 0 {
		// Merge with left sibling
		leftSibling, err := bpt.loadNode(parentNode.children[childIndex-1])
		if err != nil {
			return err
		}
		return bpt.mergeNodes(parentNode, leftSibling, childNode, childIndex-1)
	} else {
		// Merge with right sibling
		rightSibling, err := bpt.loadNode(parentNode.children[childIndex+1])
		if err != nil {
			return err
		}
		return bpt.mergeNodes(parentNode, childNode, rightSibling, childIndex)
	}
}

// borrowFromLeft borrows a key-value pair from the left sibling
func (bpt *BPlusTree) borrowFromLeft(parentNode, childNode, leftSibling *BTreeNode, childIndex int) error {
	if childNode.header.IsLeaf {
		// For leaf nodes:
		// 1. Move the rightmost key-value pair from left sibling to child
		// 2. Update the separator key in the parent

		// Get rightmost key-value pair from left sibling
		lastKeyIdx := len(leftSibling.keys) - 1
		keyToBorrow := leftSibling.keys[lastKeyIdx]
		valueToBorrow := leftSibling.values[lastKeyIdx]

		// Remove from left sibling
		leftSibling.keys = leftSibling.keys[:lastKeyIdx]
		leftSibling.values = leftSibling.values[:lastKeyIdx]

		// Insert at the beginning of child node
		childNode.keys = append([][]byte{keyToBorrow}, childNode.keys...)
		childNode.values = append([][]byte{valueToBorrow}, childNode.values...)

		// Update separator key in parent
		parentNode.keys[childIndex-1] = keyToBorrow

	} else {
		// For internal nodes:
		// 1. Move parent's separator key down to child
		// 2. Move leftSibling's rightmost key up to parent
		// 3. Move leftSibling's rightmost child pointer to child

		// Get the separator key from parent and rightmost child from left sibling
		parentKey := parentNode.keys[childIndex-1]
		lastKeyIdx := len(leftSibling.keys) - 1
		lastChildIdx := len(leftSibling.children) - 1

		// Insert parent's separator key at the beginning of child's keys
		childNode.keys = append([][]byte{parentKey}, childNode.keys...)

		// Move left sibling's rightmost child to child node's leftmost position
		childNode.children = append([]uint32{leftSibling.children[lastChildIdx]}, childNode.children...)

		// Replace parent's separator with left sibling's rightmost key
		parentNode.keys[childIndex-1] = leftSibling.keys[lastKeyIdx]

		// Remove the borrowed items from left sibling
		leftSibling.keys = leftSibling.keys[:lastKeyIdx]
		leftSibling.children = leftSibling.children[:lastChildIdx]
	}

	// Mark nodes as dirty
	leftSibling.dirty = true
	childNode.dirty = true
	parentNode.dirty = true

	// Save changes to disk
	if err := bpt.saveNode(leftSibling); err != nil {
		return err
	}
	if err := bpt.saveNode(childNode); err != nil {
		return err
	}
	return bpt.saveNode(parentNode)
}

// borrowFromRight borrows a key-value pair from the right sibling
func (bpt *BPlusTree) borrowFromRight(parentNode, childNode, rightSibling *BTreeNode, childIndex int) error {
	if childNode.header.IsLeaf {
		// For leaf nodes:
		// 1. Move the leftmost key-value pair from right sibling to child
		// 2. Update the separator key in the parent

		// Get leftmost key-value pair from right sibling
		keyToBorrow := rightSibling.keys[0]
		valueToBorrow := rightSibling.values[0]

		// Remove from right sibling
		rightSibling.keys = rightSibling.keys[1:]
		rightSibling.values = rightSibling.values[1:]

		// Add to the end of child node
		childNode.keys = append(childNode.keys, keyToBorrow)
		childNode.values = append(childNode.values, valueToBorrow)

		// Update separator key in parent
		parentNode.keys[childIndex] = rightSibling.keys[0] // First key of right sibling is the new separator

	} else {
		// For internal nodes:
		// 1. Move parent's separator key down to child
		// 2. Move rightSibling's leftmost key up to parent
		// 3. Move rightSibling's leftmost child pointer to child

		// Get the separator key from parent
		parentKey := parentNode.keys[childIndex]

		// Add parent key to child and rightSibling's leftmost child
		childNode.keys = append(childNode.keys, parentKey)
		childNode.children = append(childNode.children, rightSibling.children[0])

		// Update parent's separator with right sibling's leftmost key
		parentNode.keys[childIndex] = rightSibling.keys[0]

		// Remove the borrowed items from right sibling
		rightSibling.keys = rightSibling.keys[1:]
		rightSibling.children = rightSibling.children[1:]
	}

	// Mark nodes as dirty
	rightSibling.dirty = true
	childNode.dirty = true
	parentNode.dirty = true

	// Save changes to disk
	if err := bpt.saveNode(rightSibling); err != nil {
		return err
	}
	if err := bpt.saveNode(childNode); err != nil {
		return err
	}
	return bpt.saveNode(parentNode)
}

// mergeNodes merges two adjacent child nodes
func (bpt *BPlusTree) mergeNodes(parentNode, leftNode, rightNode *BTreeNode, leftIndex int) error {
	// Get the separator key from parent
	separatorKey := parentNode.keys[leftIndex]

	if leftNode.header.IsLeaf {
		// For leaf nodes:
		// 1. Append right node's keys and values to left node
		// 2. Update sibling pointers
		// 3. Remove separator key and right child pointer from parent

		// Append right node's keys and values to left node
		leftNode.keys = append(leftNode.keys, rightNode.keys...)
		leftNode.values = append(leftNode.values, rightNode.values...)

		// Update sibling pointers
		leftNode.header.NextPageID = rightNode.header.NextPageID

		// If right node had a next node, update its prev pointer
		if rightNode.header.NextPageID != 0 {
			nextNode, err := bpt.loadNode(rightNode.header.NextPageID)
			if err != nil {
				return err
			}
			nextNode.header.PrevPageID = leftNode.pageID
			nextNode.dirty = true
			if err := bpt.saveNode(nextNode); err != nil {
				return err
			}
		}
	} else {
		// For internal nodes:
		// 1. Append separator key from parent to left node
		// 2. Append right node's keys and children to left node
		// 3. Remove separator key and right child pointer from parent

		// Append separator key and right node's content
		leftNode.keys = append(leftNode.keys, separatorKey)
		leftNode.keys = append(leftNode.keys, rightNode.keys...)
		leftNode.children = append(leftNode.children, rightNode.children...)
	}

	// Remove separator key and right child pointer from parent
	parentNode.keys = append(parentNode.keys[:leftIndex], parentNode.keys[leftIndex+1:]...)
	parentNode.children = append(parentNode.children[:leftIndex+1], parentNode.children[leftIndex+2:]...)

	// Mark nodes as dirty
	leftNode.dirty = true
	parentNode.dirty = true

	// Free the right node's page since we've merged it
	if err := bpt.storageManager.FreePage(rightNode.pageID, BTree); err != nil {
		return err
	}

	// Save changes to disk
	if err := bpt.saveNode(leftNode); err != nil {
		return err
	}
	return bpt.saveNode(parentNode)
}

// GetRootPageID returns the page ID of the tree's root node
func (bpt *BPlusTree) GetRootPageID() uint32 {
	return bpt.rootPageID
}
