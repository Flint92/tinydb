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
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continues
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
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

	// btree callback
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// read the master page
	err = masterLoad(db)
	if err != nil {
		hasErr = int32(1)
		return fmt.Errorf("load master page: %w", err)
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

// callback for Btree, dereference a pointer
func (db *KV) pageGet(ptr uint64) BNode {
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
	if len(node.data) > BTREE_PAGE_SIZE {
		panic("bad node!")
	}

	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.data)
	return ptr
}

// callback for Btree, deallocate a page
func (db *KV) pageDel(ptr uint64) {
	if ptr >= db.page.flushed+uint64(len(db.page.temp)) {
		panic("bad ptr!")
	}

	if ptr >= db.page.flushed {
		tmpIdx := ptr - db.page.flushed
		db.page.temp = append(db.page.temp[0:tmpIdx], db.page.temp[tmpIdx+1:]...)
	} else {
		start := uint64(0)
		for _, chunk := range db.mmap.chunks {
			end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
			if ptr < end {
				offset := BTREE_PAGE_SIZE * (ptr - start)
				data := chunk[offset : offset+BTREE_PAGE_SIZE]
				_ = syscall.Munmap(data)
				return
			}
			start = end
		}
		panic("bad ptr")
	}
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
	// flush db to the disk, must be done before updating the master page
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

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
	// extend the file & mmap if needed
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFile(db, npages); err != nil {
		return err
	}

	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy data to the file
	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr).data, page)
	}
	return nil
}
