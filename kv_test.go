package tinydb

import (
	"bytes"
	"log"
	"testing"
)

const DB_PATH = "archive/testdb"

func TestKv(t *testing.T) {
	phrase1(t)
	// interrupted
	phrase2(t)
}

func phrase2(t *testing.T) {
	db, err := NewDB(DB_PATH)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	deleted, _ := db.Delete([]byte("4"))
	if deleted {
		log.Fatal("must not deleted")
	}

	tests := []struct {
		key    string
		exists bool
		value  []byte
	}{
		{
			key:    "1",
			exists: true,
			value:  []byte("Bobby"),
		},
		{
			key:    "2",
			exists: true,
			value:  []byte("Li Lei"),
		},
		{
			key:    "3",
			exists: false,
			value:  nil,
		},
		{
			key:    "4",
			exists: false,
			value:  nil,
		},
	}
	for _, test := range tests {
		if got1, got2 := db.Get([]byte(test.key)); got2 != test.exists || !bytes.Equal(got1, test.value) {
			log.Fatal("key:", test.key, " exists:", test.exists, " value:", string(got1))
		}
	}

	_ = db.Set([]byte("3"), []byte("Xiao Ming2"))
	tests = []struct {
		key    string
		exists bool
		value  []byte
	}{
		{
			key:    "1",
			exists: true,
			value:  []byte("Bobby"),
		},
		{
			key:    "2",
			exists: true,
			value:  []byte("Li Lei"),
		},
		{
			key:    "3",
			exists: true,
			value:  []byte("Xiao Ming2"),
		},
		{
			key:    "4",
			exists: false,
			value:  nil,
		},
	}
	for _, test := range tests {
		if got1, got2 := db.Get([]byte(test.key)); got2 != test.exists || !bytes.Equal(got1, test.value) {
			log.Fatal("key:", test.key, " exists:", test.exists, " value:", string(got1))
		}
	}
}

func phrase1(t *testing.T) {
	db, err := NewDB(DB_PATH)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	_ = db.Set([]byte("1"), []byte("Bobby"))
	_ = db.Set([]byte("2"), []byte("Li Lei"))
	_ = db.Set([]byte("3"), []byte("Han Meimei"))

	tests := []struct {
		key    string
		exists bool
		value  []byte
	}{
		{
			key:    "1",
			exists: true,
			value:  []byte("Bobby"),
		},
		{
			key:    "2",
			exists: true,
			value:  []byte("Li Lei"),
		},
		{
			key:    "3",
			exists: true,
			value:  []byte("Han Meimei"),
		},
		{
			key:    "4",
			exists: false,
			value:  nil,
		},
	}

	for _, test := range tests {
		if got1, got2 := db.Get([]byte(test.key)); got2 != test.exists || !bytes.Equal(got1, test.value) {
			log.Fatal("key:", test.key, " exists:", test.exists, " value:", string(got1))
		}
	}

	_ = db.Set([]byte("3"), []byte("Xiao Ming"))
	tests = []struct {
		key    string
		exists bool
		value  []byte
	}{
		{
			key:    "1",
			exists: true,
			value:  []byte("Bobby"),
		},
		{
			key:    "2",
			exists: true,
			value:  []byte("Li Lei"),
		},
		{
			key:    "3",
			exists: true,
			value:  []byte("Xiao Ming"),
		},
		{
			key:    "4",
			exists: false,
			value:  nil,
		},
	}
	for _, test := range tests {
		if got1, got2 := db.Get([]byte(test.key)); got2 != test.exists || !bytes.Equal(got1, test.value) {
			log.Fatal("key:", test.key, " exists:", test.exists, " value:", string(got1))
		}
	}

	deleted, _ := db.Delete([]byte("3"))
	if !deleted {
		log.Fatal("must deleted")
	}

	tests = []struct {
		key    string
		exists bool
		value  []byte
	}{
		{
			key:    "1",
			exists: true,
			value:  []byte("Bobby"),
		},
		{
			key:    "2",
			exists: true,
			value:  []byte("Li Lei"),
		},
		{
			key:    "3",
			exists: false,
			value:  nil,
		},
		{
			key:    "4",
			exists: false,
			value:  nil,
		},
	}
	for _, test := range tests {
		if got1, got2 := db.Get([]byte(test.key)); got2 != test.exists || !bytes.Equal(got1, test.value) {
			log.Fatal("key:", test.key, " exists:", test.exists, " value:", string(got1))
		}
	}
}
