package tinydb

import "encoding/binary"

const (
	BNODE_FREE_LIST  = 3
	FREE_LIST_HEADER = 4 + 8 + 8 // type(2B) + size(2B) + total(8B) + next(8B)
	FREE_LIST_CAP    = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) >> 3
)

// Freelist The node format:
// | type | size | total | next | pointers |
// | 2B | 2B | 8B | 8B | size * 8B |
// Multiple pointers to unused pages
// the link to the next node
// the total number of items in the list, only applied to the head node
type Freelist struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode      // dereference a pointer
	new func(node BNode) uint64 // append a new page
	use func(uint64, BNode)     // reuse a page
}

// Total number of items in the list
func (fl *Freelist) Total() uint64 {
	return binary.LittleEndian.Uint64(fl.get(fl.head).data[HEADER:])
}

// Get return the nth pointer
func (fl *Freelist) Get(topn int) uint64 {
	assert(0 <= topn && uint64(topn) < fl.Total(), "bad topn!")

	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		assert(next != 0, "next must exists!")
		node = fl.get(next)
	}

	return flnPtr(node, flnSize(node)-topn-1)
}

// Update remove `popn` pointers and add some new pointers
func (fl *Freelist) Update(popn int, freed []uint64) {
	assert(0 <= popn && uint64(popn) <= fl.Total(), "bad popn!")

	if popn == 0 && len(freed) == 0 {
		return // nothing to do
	}

	// prepare to construct the new list
	total := fl.Total()
	var reuse []uint64

	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recycle the node itself

		if popn >= flnSize(node) {
			// phase 1
			// remove all pointers in this mode
			popn -= flnSize(node)
		} else {
			// phase 2
			// remove some pointers
			remain := flnSize(node) - popn
			popn = 0
			// reuse pointers from the free list itself

			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}

			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}

		// discard the node and move to the next node
		total -= uint64(flnSize(node))
		fl.head = flnNext(node)
	}

	assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0, "pop error")

	// phase3: prepend new nodes
	flPush(fl, freed, reuse)

	// done
	flnSetTotal(fl.get(fl.head), total+uint64(len(freed)))
}

func flPush(fl *Freelist, freed, reuse []uint64) {
	for len(freed) > 0 {
		newNode := NewBNode(make([]byte, BTREE_PAGE_SIZE))

		// construct a new node
		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(newNode, uint16(size), fl.head)

		for i, ptr := range freed[:size] {
			flnSetPtr(newNode, i, ptr)
		}

		freed = freed[size:]

		if len(reuse) > 0 {
			// reuse a pinter from the list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, newNode)
		} else {
			// append a page to house the new node
			fl.head = fl.new(newNode)
		}
	}

	assert(len(reuse) == 0, "reuse must empty!")
}

func flnSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node.data[2:]))
}

func flnNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node.data[12:])
}

func flnPtr(node BNode, idx int) uint64 {
	offset := FREE_LIST_HEADER + idx*8
	return binary.LittleEndian.Uint64(node.data[offset:])
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	offset := FREE_LIST_HEADER + idx*8
	binary.LittleEndian.PutUint64(node.data[offset:], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[0:], BNODE_FREE_LIST)
	binary.LittleEndian.PutUint16(node.data[2:], size)
	binary.LittleEndian.PutUint64(node.data[12:], next)
}

func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[4:12], total)
}
