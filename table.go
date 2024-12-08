package tinydb

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// TDEF_META internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// TDEF_TABLE internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// Value table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// Record table row
type Record struct {
	Cols []string
	Vals []Value
}

// TableDef table definition
type TableDef struct {
	// user defined
	Name  string
	Types []uint32 // column types
	Cols  []string // column names
	PKeys int      // the first `PKeys` columns are the primary key
	// auto-assigned  B-tree key prefixes for different table
	Prefix uint32
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{
		Type: TYPE_BYTES,
		Str:  val,
	})
	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{
		Type: TYPE_INT64,
		I64:  val,
	})
	return rec
}

func (rec *Record) Get(key string) *Value {
	idx := -1
	for i, col := range rec.Cols {
		if col == key {
			idx = i
			break
		}
	}

	if idx == -1 {
		return nil
	}
	return &rec.Vals[idx]
}
