package tinydb

import (
	"bytes"
	"encoding/binary"
)

const (
	HEADER     = 4 // type(2B) + size(2B)
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

type BNode struct {
	data []byte // can be dumped to the disk
}

func NewBNode(data []byte) BNode {
	return BNode{data: data}
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, data uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], data)
}

// offset list
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0 // the offset of first k-v pair is always zero
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to leaf node
func leafInsert(new, old BNode, idx uint16, key, value []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, value)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// update kv to leaf node
func leafUpdate(new, old BNode, idx uint16, key, value []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, value)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// remove a key from a leaf node
func leafDelete(new, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}

// copy multiple KVs into the position
func nodeAppendRange(new, old BNode, dstNew, srcOld uint16, n uint16) {
	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key, value []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(value)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], value)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(value)))
}

// split a node if it's too big, the result are 1-3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := NewBNode(make([]byte, BTREE_PAGE_SIZE<<1)) // might be split later
	right := NewBNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := NewBNode(make([]byte, BTREE_PAGE_SIZE))
	middle := NewBNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)

	return 3, [3]BNode{leftleft, middle, right}
}

func nodeSplit2(left, right, old BNode) {
	nkeys := old.nkeys()
	nbytes := old.nbytes()

	rightCount := uint16(1)
	for ; rightCount <= nkeys; rightCount++ {
		rightBytes := HEADER + 8*rightCount + 2*rightCount + (nbytes - old.getOffset(nkeys-rightCount))
		if rightBytes == BTREE_PAGE_SIZE {
			break
		}
		if rightBytes > BTREE_PAGE_SIZE {
			rightCount -= 1
			break
		}
	}

	idx := nkeys - rightCount

	left.setHeader(old.btype(), idx)
	nodeAppendRange(left, old, 0, 0, idx)

	right.setHeader(old.btype(), nkeys-idx)
	nodeAppendRange(right, old, 0, idx, nkeys-idx)
}

// merge 2 node into 1
func nodeMerge(new, left, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.btype())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}
