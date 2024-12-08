package tinydb

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
	free Freelist
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continues
	}
	page struct {
		flushed uint64 // database size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer
		// nil value denotes a deallocated page
		updates map[uint64][]byte
	}
}

func NewDB(path string) (*KV, error) {
	db := &KV{Path: path}
	err := db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open file %s: %w", db.Path, err)
	}
	db.fp = fp

	hasErr := int32(0)

	defer func(hasError *int32) {
		if *hasError != 0 {
			db.Close()
		}
	}(&hasErr)

	// create the initial mmap
	sz, chunk, err := mmapInit(fp)
	if err != nil {
		hasErr = int32(1)
		return fmt.Errorf("mmap init: %w", err)
	}

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	db.page.updates = make(map[uint64][]byte)

	// btree callback
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// free list callback
	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse

	// read the master page
	err = masterLoad(db)
	if err != nil {
		hasErr = int32(1)
		return fmt.Errorf("load master page: %w", err)
	}

	if db.free.head == 0 && len(db.page.updates) == 0 {
		node := NewBNode(make([]byte, BTREE_PAGE_SIZE))
		db.free.head = db.free.new(node)
		db.pageUse(db.free.head, node)
	}

	return nil
}

// Get read the db by the key
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// Set update the k-v to the db
func (db *KV) Set(key []byte, value []byte) error {
	db.tree.Insert(key, value)
	return flushPages(db)
}

// Delete remove the key to the db
func (db *KV) Delete(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		_ = syscall.Munmap(chunk)
	}
	_ = db.fp.Close()
}

// create the initial mmap that covers the whole file.
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}
	mmapSize := 64 << 20
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmapSize can be larger than the file
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
}

// callback for Btree & FreeList, dereference a pointer
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		return NewBNode(page) // for new pages
	}
	return db.pageGetMapped(ptr) // for written pages
}

func (db *KV) pageGetMapped(ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return NewBNode(chunk[offset : offset+BTREE_PAGE_SIZE])
		}
		start = end
	}
	panic("bad ptr")
}

// callback for Btree, allocate a new page
func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE, "bad node!")

	ptr := uint64(0)
	if uint64(db.page.nfree) < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
		db.pageUse(ptr, node)
	} else {
		// append a new page
		ptr = db.pageAppend(node)
	}

	return ptr
}

// callback for Btree, deallocate a page
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// callback for FreeList, allocate
func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE, "bad node!")

	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

// callback for FreeList, reuse a page
func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}

// extend the file to at least `npages`
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		// the file size is increased exponentially
		// so what we don't hava to extend the file for every update
		inc := filePages >> 3
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	fileSize := filePages * BTREE_PAGE_SIZE

	err := fallocate(db.fp, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}

// extend the mmap by adding new mappings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap : %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	return nil
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func syncPages(db *KV) error {
	// flush db to the disk
	pageNewCount := uint64(0)
	for _, page := range db.page.updates {
		if page != nil {
			pageNewCount++
		}
	}

	db.page.flushed += pageNewCount
	db.page.updates = make(map[uint64][]byte)

	// update & flush the master page
	if err := materStore(db); err != nil {
		return fmt.Errorf("materstore: %w", err)
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	return nil
}

func writePages(db *KV) error {
	// update the free list
	var freed []uint64
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	// extend the file & mmap if needed
	npages := int(db.page.flushed) + db.page.nappend
	if err := extendFile(db, npages); err != nil {
		return err
	}

	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy data to the file
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(db.pageGetMapped(ptr).data, page)
		}
	}
	return nil
}
