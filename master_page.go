package tinydb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const DB_SIG = "TINYDB_SIG"

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 16B | 8B 		  | 8B        |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write
		db.page.flushed = 1 // reserved for the master page
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])

	sig := make([]byte, 16)
	copy(sig[:], []byte(DB_SIG))
	//verify the page
	if !bytes.Equal(sig, data[:16]) {
		return errors.New("bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("bad master page")
	}

	db.tree.root = root
	db.page.flushed = used
	return nil
}

// update the master page. it must be atomic
func materStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)

	// NOTE: Updating the page via mmap is not atomic.
	// 		 Use the `pwrite()` syscall instead
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}
