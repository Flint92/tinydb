package tinydb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

const DB_PATH = "archive/testdb"
const TABLE_NAME = "test"

func TestDB(t *testing.T) {
	db, err := Open(DB_PATH)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	tdef := &TableDef{
		Name:  TABLE_NAME,
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64, TYPE_INT64},
		Cols:  []string{"id", "name", "age", "ext"},
		PKeys: 1,
	}

	_ = db.TableNew(tdef)

	rec := &Record{
		Cols: []string{"id"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(1),
			},
		},
	}

	got, _ := db.Get(TABLE_NAME, rec)
	require.False(t, got, "record should not exist")

	rec = &Record{
		Cols: []string{"id", "name", "age", "ext"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(1),
			},
			{
				Type: TYPE_BYTES,
				Str:  []byte("Bobby"),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(18),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(-1),
			},
		},
	}
	got, _ = db.Insert(TABLE_NAME, *rec)
	require.True(t, got, "insert success at first time")

	got, _ = db.Insert(TABLE_NAME, *rec)
	require.False(t, got, "insert fail at second time")

	rec = &Record{
		Cols: []string{"id", "name", "age", "ext"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(1),
			},
			{
				Type: TYPE_BYTES,
				Str:  []byte("Bobby New"),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(18),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(-2),
			},
		},
	}

	got, _ = db.Update(TABLE_NAME, *rec)
	require.True(t, got, "update success at first time")

	rec = &Record{
		Cols: []string{"id", "name", "age", "ext"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(2),
			},
			{
				Type: TYPE_BYTES,
				Str:  []byte("Bobby New"),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(18),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(-2),
			},
		},
	}

	got, _ = db.Update(TABLE_NAME, *rec)
	require.False(t, got, "update fail at first time")

	rec = &Record{
		Cols: []string{"id"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(2),
			},
		},
	}
	got, _ = db.Delete(TABLE_NAME, *rec)
	require.False(t, got, "delete fail at first time")

	rec = &Record{
		Cols: []string{"id", "name", "age", "ext"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(1),
			},
			{
				Type: TYPE_BYTES,
				Str:  []byte("Bobby New"),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(18),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(-3),
			},
		},
	}

	got, _ = db.Upsert(TABLE_NAME, *rec)
	require.True(t, got, "upsert success at first time")

	rec = &Record{
		Cols: []string{"id", "name", "age", "ext"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(2),
			},
			{
				Type: TYPE_BYTES,
				Str:  []byte("Bobby New"),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(18),
			},
			{
				Type: TYPE_INT64,
				I64:  int64(-2),
			},
		},
	}

	got, _ = db.Upsert(TABLE_NAME, *rec)
	require.True(t, got, "upsert success at second time")

	rec = &Record{
		Cols: []string{"id"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(2),
			},
		},
	}
	got, _ = db.Get(TABLE_NAME, rec)
	require.True(t, got, "get success after upsert")
	require.Equal(t, rec.Get("id").I64, int64(2))
	require.Equal(t, rec.Get("name").Str, []byte("Bobby New"))
	require.Equal(t, rec.Get("age").I64, int64(18))
	require.Equal(t, rec.Get("ext").I64, int64(-2))

	rec = &Record{
		Cols: []string{"id"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(1),
			},
		},
	}
	got, _ = db.Get(TABLE_NAME, rec)
	require.True(t, got, "get success after upsert")
	require.Equal(t, rec.Get("id").I64, int64(1))
	require.Equal(t, rec.Get("name").Str, []byte("Bobby New"))
	require.Equal(t, rec.Get("age").I64, int64(18))
	require.Equal(t, rec.Get("ext").I64, int64(-3))

	rec = &Record{
		Cols: []string{"id"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(1),
			},
		},
	}
	got, _ = db.Delete(TABLE_NAME, *rec)
	require.True(t, got, "delete success")

	rec = &Record{
		Cols: []string{"id"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  int64(2),
			},
		},
	}
	got, _ = db.Delete(TABLE_NAME, *rec)
	require.True(t, got, "delete success")

}
