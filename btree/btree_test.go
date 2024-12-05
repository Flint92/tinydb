package btree

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"slices"
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, _ := pages[ptr]
				return node
			},
			new: func(node BNode) uint64 {
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) printTree() {
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Println("Pointer:", pt)
		fmt.Println("BNode data:", node.data)
	}
}

func TestBtree(t *testing.T) {
	test := make([]byte, 4096)

	c := newC()
	c.add("a", "1111")
	require.NotNil(t, c.tree.root)

	copy(test, []byte{2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 0, 0, 0, 0, 1, 0, 4, 0, 97, 49, 49, 49, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.add("b", "2222")
	copy(test, []byte{2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 22, 0, 0, 0, 0, 0, 1, 0, 4, 0, 97, 49, 49, 49, 49, 1, 0, 4, 0, 98, 50, 50, 50, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.add("b", "3333")
	copy(test, []byte{2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 22, 0, 0, 0, 0, 0, 1, 0, 4, 0, 97, 49, 49, 49, 49, 1, 0, 4, 0, 98, 51, 51, 51, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.add("a", "4444")
	copy(test, []byte{2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 22, 0, 0, 0, 0, 0, 1, 0, 4, 0, 97, 52, 52, 52, 52, 1, 0, 4, 0, 98, 51, 51, 51, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.del("c")
	copy(test, []byte{2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 22, 0, 0, 0, 0, 0, 1, 0, 4, 0, 97, 52, 52, 52, 52, 1, 0, 4, 0, 98, 51, 51, 51, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.del("b")
	copy(test, []byte{2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 0, 0, 0, 0, 1, 0, 4, 0, 97, 52, 52, 52, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.del("a")
	copy(test, []byte{2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))

	c.add("d", "5555")
	copy(test, []byte{2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 13, 0, 0, 0, 0, 0, 1, 0, 4, 0, 100, 53, 53, 53, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, slices.Equal(test, c.tree.get(c.tree.root).data))
}
