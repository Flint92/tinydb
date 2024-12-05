package btree

import "bytes"

const (
	BTREE_PAGE_SIZE           = 4096 // page size is defined to 4K
	BTREE_PAGE_MAX_KEY_SIZE   = 1000
	BTREE_PAGE_MAX_VALUE_SIZE = 3000
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // deference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

func (tree *BTree) Delete(key []byte) bool {
	if len(key) == 0 || len(key) > BTREE_PAGE_MAX_KEY_SIZE {
		panic("bad key!")
	}

	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}

	return true
}

func (tree *BTree) Insert(key, value []byte) {
	if len(key) == 0 || len(key) > BTREE_PAGE_MAX_KEY_SIZE {
		panic("bad key!")
	}
	if len(value) > BTREE_PAGE_MAX_VALUE_SIZE {
		panic("bad value!")
	}

	if tree.root == 0 {
		// create the first node
		root := NewBNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, value)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	node = treeInsert(tree, node, key, value)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		// the root was split, add a new level
		root := NewBNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted {
			ptr, kk := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, kk, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}

}

// insert a KV into a node, the result might be split into 2 nodes
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes
func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	// the result node
	// it's allowed to be bigger than 1 page and will be split if so
	newNode := NewBNode(make([]byte, BTREE_PAGE_SIZE<<1))

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(newNode, node, idx, key, val)
		} else {
			// insert it after
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node
		nodeInsert(tree, newNode, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return newNode
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		newNode := NewBNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(newNode, node, idx)
		return newNode
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}

	tree.del(kptr)

	newNode := NewBNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	if mergeDir < 0 { // left
		merged := NewBNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplaceKid2(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	} else if mergeDir > 0 { // right
		merged := NewBNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplaceKid2(newNode, node, idx, tree.new(merged), merged.getKey(0))
	} else {
		if updated.nkeys() == 0 {
			// kid is empty after deletion and has no sibling to merge with.
			// this happens when its parent has only one kid.
			// discard the empty kid and return the parent as an empty node.
			newNode.setHeader(BNODE_NODE, 0)
			// the empty node will be eliminated before reaching root.
		} else {
			nodeReplaceKidN(tree, newNode, node, idx, updated)
		}
	}
	return newNode
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE>>2 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}

	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}

	return 0, BNode{}
}

func nodeReplaceKid2(new BNode, old BNode, idx uint16, merged uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, merged, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

func nodeInsert(tree *BTree, new, node BNode, idx uint16, key, val []byte) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
