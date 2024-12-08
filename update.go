package tinydb

import "fmt"

type UpdateMode int

// modes of the updates
const (
	MODE_UPSERT      = UpdateMode(0) // insert or replace
	MODE_UPDATE_ONLY = UpdateMode(1) // update existing keys
	MODE_INSERT_ONLY = UpdateMode(3) // only add new keys
)

func Update(db *KV, key, val []byte, mode UpdateMode) (bool, error) {
	switch mode {
	case MODE_UPSERT:
		err := db.Set(key, val)
		if err != nil {
			return false, err
		}
		return true, nil
	case MODE_UPDATE_ONLY:
		if _, ok := db.Get(key); !ok {
			return false, fmt.Errorf("the key %s does not exist", key)
		}
		err := db.Set(key, val)
		if err != nil {
			return false, err
		}
		return true, nil
	case MODE_INSERT_ONLY:
		if _, ok := db.Get(key); ok {
			return false, fmt.Errorf("the key %s exists", key)
		}
		err := db.Set(key, val)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, fmt.Errorf("unknown mode %d", mode)
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode UpdateMode) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	return Update(db.kv, key, val, mode)
}
