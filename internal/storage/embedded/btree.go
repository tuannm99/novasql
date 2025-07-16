// sqlite like storage
package embedded

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	MaxKeysPerNode = 4
	MinKeysPerNode = 2
)

type OverflowPage struct {
	NextPage int64
	Data     []byte
}

type BTreeNode struct {
	IsLeaf     bool
	Keys       []int64
	Values     []int64
	Children   []int64
	NextLeaf   int64
	Parent     int64
	PageNumber int64
}

type BTreeIndex struct {
	rootPageNum int64
	pager       *Pager
}

func NewBTree(pager *Pager) (*BTreeIndex, error) {
	rootNode := &BTreeNode{
		IsLeaf:     true,
		Keys:       make([]int64, 0, MaxKeysPerNode),
		Values:     make([]int64, 0, MaxKeysPerNode),
		Children:   make([]int64, 0, MaxKeysPerNode+1),
		NextLeaf:   -1,
		Parent:     -1,
		PageNumber: 0,
	}

	if err := writeNode(pager, rootNode); err != nil {
		return nil, fmt.Errorf("write root node: %w", err)
	}

	return &BTreeIndex{
		rootPageNum: 0,
		pager:       pager,
	}, nil
}

func (t *BTreeIndex) Insert(key, value int64) error {
	root, err := readNode(t.pager, t.rootPageNum)
	if err != nil {
		return fmt.Errorf("read root node: %w", err)
	}

	if len(root.Keys) == MaxKeysPerNode {
		newRoot := &BTreeNode{
			IsLeaf:     false,
			Keys:       make([]int64, 0, MaxKeysPerNode),
			Values:     make([]int64, 0, MaxKeysPerNode),
			Children:   make([]int64, 0, MaxKeysPerNode+1),
			Parent:     -1,
			PageNumber: int64(t.pager.PageCount()),
		}

		mid := MaxKeysPerNode / 2
		newRoot.Keys = append(newRoot.Keys, root.Keys[mid])
		newRoot.Children = append(newRoot.Children, root.PageNumber)

		rightNode := &BTreeNode{
			IsLeaf:     root.IsLeaf,
			Keys:       root.Keys[mid+1:],
			Values:     root.Values[mid+1:],
			Children:   root.Children[mid+1:],
			Parent:     newRoot.PageNumber,
			PageNumber: int64(t.pager.PageCount() + 1),
		}

		root.Keys = root.Keys[:mid]
		root.Values = root.Values[:mid]
		root.Children = root.Children[:mid+1]
		root.Parent = newRoot.PageNumber

		if err := writeNode(t.pager, root); err != nil {
			return fmt.Errorf("write root node: %w", err)
		}
		if err := writeNode(t.pager, rightNode); err != nil {
			return fmt.Errorf("write right node: %w", err)
		}
		if err := writeNode(t.pager, newRoot); err != nil {
			return fmt.Errorf("write new root: %w", err)
		}

		t.rootPageNum = newRoot.PageNumber
		root = newRoot
	}

	return t.insertIntoNode(root, key, value)
}

func (t *BTreeIndex) insertIntoNode(node *BTreeNode, key, value int64) error {
	if node.IsLeaf {

		pos := 0
		for pos < len(node.Keys) && node.Keys[pos] < key {
			pos++
		}

		node.Keys = append(node.Keys, 0)
		node.Values = append(node.Values, 0)
		copy(node.Keys[pos+1:], node.Keys[pos:])
		copy(node.Values[pos+1:], node.Values[pos:])
		node.Keys[pos] = key
		node.Values[pos] = value

		return writeNode(t.pager, node)
	}

	childPos := 0
	for childPos < len(node.Keys) && key >= node.Keys[childPos] {
		childPos++
	}

	child, err := readNode(t.pager, node.Children[childPos])
	if err != nil {
		return fmt.Errorf("read child node: %w", err)
	}

	if len(child.Keys) == MaxKeysPerNode {
		mid := MaxKeysPerNode / 2
		medianKey := child.Keys[mid]

		rightNode := &BTreeNode{
			IsLeaf:     child.IsLeaf,
			Keys:       child.Keys[mid+1:],
			Values:     child.Values[mid+1:],
			Children:   child.Children[mid+1:],
			Parent:     node.PageNumber,
			PageNumber: int64(t.pager.PageCount()),
		}

		child.Keys = child.Keys[:mid]
		child.Values = child.Values[:mid]
		child.Children = child.Children[:mid+1]

		node.Keys = append(node.Keys, 0)
		node.Values = append(node.Values, 0)
		node.Children = append(node.Children, 0)
		copy(node.Keys[childPos+1:], node.Keys[childPos:])
		copy(node.Values[childPos+1:], node.Values[childPos:])
		copy(node.Children[childPos+2:], node.Children[childPos+1:])
		node.Keys[childPos] = medianKey
		node.Children[childPos+1] = rightNode.PageNumber

		if err := writeNode(t.pager, child); err != nil {
			return fmt.Errorf("write child node: %w", err)
		}
		if err := writeNode(t.pager, rightNode); err != nil {
			return fmt.Errorf("write right node: %w", err)
		}
		if err := writeNode(t.pager, node); err != nil {
			return fmt.Errorf("write parent node: %w", err)
		}

		if key >= medianKey {
			child = rightNode
		}
	}

	return t.insertIntoNode(child, key, value)
}

func (t *BTreeIndex) Search(key int64) (int64, error) {
	node, err := readNode(t.pager, t.rootPageNum)
	if err != nil {
		return -1, fmt.Errorf("read root node: %w", err)
	}

	for {

		pos := 0
		for pos < len(node.Keys) && node.Keys[pos] < key {
			pos++
		}

		if pos < len(node.Keys) && node.Keys[pos] == key {
			return node.Values[pos], nil
		}

		if node.IsLeaf {
			return -1, nil
		}

		node, err = readNode(t.pager, node.Children[pos])
		if err != nil {
			return -1, fmt.Errorf("read child node: %w", err)
		}
	}
}

func writeNode(pager *Pager, node *BTreeNode) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, node.IsLeaf); err != nil {
		return fmt.Errorf("write is leaf: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, int32(len(node.Keys))); err != nil {
		return fmt.Errorf("write keys length: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, node.Keys); err != nil {
		return fmt.Errorf("write keys: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, node.Values); err != nil {
		return fmt.Errorf("write values: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, node.Children); err != nil {
		return fmt.Errorf("write children: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, node.NextLeaf); err != nil {
		return fmt.Errorf("write next leaf: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, node.Parent); err != nil {
		return fmt.Errorf("write parent: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, node.PageNumber); err != nil {
		return fmt.Errorf("write page number: %w", err)
	}

	data := buf.Bytes()
	if len(data) > pager.PageSize() {
		return fmt.Errorf("node data exceeds page size")
	}

	padding := make([]byte, pager.PageSize()-len(data))
	data = append(data, padding...)

	return pager.WritePage(int(node.PageNumber), data)
}

func readNode(pager *Pager, pageNum int64) (*BTreeNode, error) {
	page, err := pager.GetPage(int(pageNum))
	if err != nil {
		return nil, fmt.Errorf("get page: %w", err)
	}

	buf := bytes.NewReader(page.Data)

	var isLeaf bool
	if err := binary.Read(buf, binary.LittleEndian, &isLeaf); err != nil {
		return nil, fmt.Errorf("read is leaf: %w", err)
	}

	var numKeys int32
	if err := binary.Read(buf, binary.LittleEndian, &numKeys); err != nil {
		return nil, fmt.Errorf("read keys length: %w", err)
	}

	keys := make([]int64, numKeys)
	if err := binary.Read(buf, binary.LittleEndian, &keys); err != nil {
		return nil, fmt.Errorf("read keys: %w", err)
	}

	values := make([]int64, numKeys)
	if err := binary.Read(buf, binary.LittleEndian, &values); err != nil {
		return nil, fmt.Errorf("read values: %w", err)
	}

	var numChildren int32
	if !isLeaf {
		numChildren = numKeys + 1
	}
	children := make([]int64, numChildren)
	if err := binary.Read(buf, binary.LittleEndian, &children); err != nil {
		return nil, fmt.Errorf("read children: %w", err)
	}

	var nextLeaf int64
	if err := binary.Read(buf, binary.LittleEndian, &nextLeaf); err != nil {
		return nil, fmt.Errorf("read next leaf: %w", err)
	}

	var parent int64
	if err := binary.Read(buf, binary.LittleEndian, &parent); err != nil {
		return nil, fmt.Errorf("read parent: %w", err)
	}

	var pageNumber int64
	if err := binary.Read(buf, binary.LittleEndian, &pageNumber); err != nil {
		return nil, fmt.Errorf("read page number: %w", err)
	}

	return &BTreeNode{
		IsLeaf:     isLeaf,
		Keys:       keys,
		Values:     values,
		Children:   children,
		NextLeaf:   nextLeaf,
		Parent:     parent,
		PageNumber: pageNumber,
	}, nil
}

func (t *BTreeIndex) writeOverflowChain(data []byte) (int64, error) {
	if len(data) == 0 {
		return -1, nil
	}

	pageSize := t.pager.PageSize() - 8
	firstPageNum := int64(t.pager.PageCount())
	currentPageNum := firstPageNum

	for len(data) > 0 {
		chunkSize := min(pageSize, len(data))

		pageData := make([]byte, t.pager.PageSize())
		nextPage := int64(-1)
		if len(data) > chunkSize {
			nextPage = currentPageNum + 1
		}
		binary.LittleEndian.PutUint64(pageData[0:8], uint64(nextPage))
		copy(pageData[8:], data[:chunkSize])

		if err := t.pager.WritePage(int(currentPageNum), pageData); err != nil {
			return -1, fmt.Errorf("write overflow page: %w", err)
		}

		data = data[chunkSize:]
		currentPageNum++
	}

	return firstPageNum, nil
}

func (t *BTreeIndex) readOverflowChain(firstPageNum int64) ([]byte, error) {
	if firstPageNum == -1 {
		return nil, nil
	}

	var result []byte
	currentPageNum := firstPageNum

	for currentPageNum != -1 {
		page, err := t.pager.GetPage(int(currentPageNum))
		if err != nil {
			return nil, fmt.Errorf("read overflow page: %w", err)
		}

		nextPage := int64(binary.LittleEndian.Uint64(page.Data[0:8]))

		result = append(result, page.Data[8:]...)

		currentPageNum = nextPage
	}

	return result, nil
}

func (t *BTreeIndex) InsertWithOverflow(key int64, value []byte) error {
	valuePageNum, err := t.writeOverflowChain(value)
	if err != nil {
		return fmt.Errorf("write overflow chain: %w", err)
	}

	return t.Insert(key, valuePageNum)
}

func (t *BTreeIndex) SearchWithOverflow(key int64) ([]byte, error) {
	valuePageNum, err := t.Search(key)
	if err != nil {
		return nil, err
	}
	if valuePageNum == -1 {
		return nil, nil
	}

	return t.readOverflowChain(valuePageNum)
}
