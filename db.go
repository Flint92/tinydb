package tinydb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const TABLE_PREFIX_MIN = 0

type DB struct {
	Path string
	// internals
	kv     *KV
	tables map[string]*TableDef // cached table definition
}

func Open(Path string) (*DB, error) {
	kv, err := NewDB(Path)
	if err != nil {
		return nil, err
	}

	return &DB{
		Path: Path,
		kv:   kv,
	}, nil
}

func (db *DB) Close() {
	db.kv.Close()
}

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	assert(err == nil, "error never happened")
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// allocate a new prefix
	assert(tdef.Prefix == 0, "tdef prefix should be 0")
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	assert(err == nil, "error never happened")
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > TABLE_PREFIX_MIN, "bad tdef prefix")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, TDEF_META, *meta, MODE_UPSERT)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	assert(err == nil, "error never happened")
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, MODE_UPSERT)
	return err
}

// Get get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table %s not found", table)
	}

	return dbGet(db, tdef, rec)
}

// Set add a record
func (db *DB) Set(table string, rec Record, mode UpdateMode) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func tableDefCheck(tdef *TableDef) error {
	return nil
}

// get the table definition by name
func getTableDef(db *DB, table string) *TableDef {
	tdef, ok := db.tables[table]
	if !ok {
		if db.tables == nil {
			db.tables = make(map[string]*TableDef)
		}
		tdef = getTableDefDB(db, table)
		if tdef != nil {
			db.tables[table] = tdef
		}
	}

	return tdef
}

func getTableDefDB(db *DB, table string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(table))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert(err == nil, "error must never happened")
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil, "error must never happened")
	return tdef
}
