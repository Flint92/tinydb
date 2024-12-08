package tinydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}

	values = decodeValues(val, values[tdef.PKeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values...)

	return true, nil
}

// for primary key
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("what?")
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) []Value {
	var cur int
	var result []Value
	for _, valDef := range out {
		if TYPE_BYTES == valDef.Type {
			nullTerm := cur
			for nullTerm < len(in) && in[nullTerm] != 0 {
				nullTerm++
			}
			str := unescapeString(in[cur:nullTerm])
			valDef.Str = str
			cur = nullTerm + 1 // 移动到下一个值的位置

		}
		if TYPE_INT64 == valDef.Type {
			buf := in[cur : cur+8]
			u := binary.BigEndian.Uint64(buf)
			v := int64(u - (1 << 63))
			valDef.I64 = v
			cur += 8
		}

		if TYPE_ERROR == valDef.Type {
			panic("bad type")
		}

		result = append(result, valDef)
	}
	return result
}

func unescapeString(in []byte) []byte {
	var out []byte
	i := 0
	for i < len(in) {
		if in[i] == 0x01 && i+1 < len(in) {
			if in[i+1] == 0x01 {
				out = append(out, 0)
				i += 2
			} else if in[i+1] == 0x02 {
				out = append(out, 1)
				i += 2
			} else {
				out = append(out, in[i])
				i++
			}
		} else {
			out = append(out, in[i])
			i++
		}
	}
	return out
}

// Strings are encoded as null terminated strings,
// escape the nul byte so that strings contain no nul byte.
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

// reorder a record and check for missing columns.
// n == tdef.PKeys: record is exactly a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, record Record, n int) ([]Value, error) {
	reorderedRec := make([]Value, len(tdef.Cols))
	cols := tdef.Cols[:n]
	for i := range n {
		if cols[i] != record.Cols[i] {
			return nil, fmt.Errorf("tinydb: invalid column name: %s", record.Cols[i])
		}

		typ := tdef.Types[i]

		if TYPE_ERROR == typ {
			return nil, fmt.Errorf("tinydb: invalid column type: %s", record.Cols[i])
		}

		recVal := record.Get(record.Cols[i])
		if recVal == nil || recVal.Type != typ {
			return nil, fmt.Errorf("tinydb: invalid column type: %s", record.Cols[i])
		}

		reorderedRec[i] = *recVal
	}

	return reorderedRec, nil
}
