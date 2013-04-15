package schemalessql

import (
	"bytes"
	"database/sql"
	//	"database/sql/driver"
	"encoding/gob"
	"errors"
	//"log"
	"reflect"
	"sync"
	"time"
)

var (
	ErrCouldNotSetup     = errors.New("schemalessql: required tables/indexes could not be created")
	ErrInvalidEntityType = errors.New("schemalessql: invalid entity type")
	ErrUnsupportedType   = errors.New("schemalessql: unsupported struct field type")
)

var (
	EntityTable = "entities"
	IndexPrefix = "index"
)

type Datastore struct {
	*sql.DB
	structure struct {
		sync.RWMutex
		created map[reflect.Type]bool
		codec   map[reflect.Type]map[string]string
	}
}

func Open(driverName, dataSourceName string) (*Datastore, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	d := Datastore{sql.DB: db}
	d.structure.created = make(map[reflect.Type]bool)
	d.structure.codec = make(map[reflect.Type]map[string]string)
	return &d, nil
}

// Register creates entitiy and index tables with suitable types
func (d *Datastore) Register(src interface{}) error {
	// check if already registered

	t := reflect.TypeOf(src)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	d.structure.RLock()
	if d.structure.created[t] {
		d.structure.RUnlock()
		// existing type
		return nil
	}

	d.structure.RUnlock()
	d.structure.Lock()
	defer d.structure.Unlock()

	// new type, create entity table
	tx, err := d.Begin()
	if err != nil {
		return ErrCouldNotSetup
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS '` + EntityTable + `' ('id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 'data' BLOB NOT NULL)`); err != nil {
		return ErrCouldNotSetup
	}

	if _, err := tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS 'id_index' ON '` + EntityTable + `' ('id' ASC)`); err != nil {
		return ErrCouldNotSetup
	}

	// create index tables for registered reflect.Type
	v := reflect.ValueOf(src)
	n := t.NumField()

	d.structure.codec[t] = make(map[string]string)

	for i := 0; i < n; i++ {
		vt := t.Field(i)

		// register type for gob
		gob.Register(v.Field(i).Interface())

		if vt.Tag.Get("datastore") == "noindex" {
			continue
		}

		vf := v.Field(i)
		var fieldtype string

		switch /*vi :=*/ vf.Interface().(type) {
		case time.Time:
			fieldtype = "DATETIME"
		case []byte:
			fieldtype = "BLOB"
		default:
			switch vf.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldtype = "INTEGER"
			case reflect.Float32, reflect.Float64:
				fieldtype = "FLOAT"
			case reflect.Bool:
				fieldtype = "BOOL"
			case reflect.String:
				fieldtype = "TEXT"
			default:
				return ErrUnsupportedType
			}
		}

		fieldname := vt.Name
		d.structure.codec[t][fieldname] = fieldtype

		if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS '` + IndexPrefix + `_` + fieldname + `' ('entitiy_id' INTEGER NOT NULL UNIQUE, 'value' ` + fieldtype + `)`); err != nil {
			return ErrCouldNotSetup
		}

		if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS 'id_value_index' ON '` + IndexPrefix + `_` + fieldname + `' ('entitiy_id' ASC, 'value' ASC)`); err != nil {
			return ErrCouldNotSetup
		}

	}

	tx.Commit()
	d.structure.created[t] = true
	return nil
}

type Key struct {
	int64
}

func (d *Datastore) Put(key *Key, src interface{}) (*Key, error) {
	if err := d.Register(src); err != nil {
		return key, err
	}

	// encode data
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(src); err != nil {
		return key, err
	}

	// begin transaction
	tx, err := d.Begin()
	if err != nil {
		return key, err
	}
	defer tx.Rollback()

	if key == nil {
		// insert data
		stmt, err := tx.Prepare(`INSERT INTO '` + EntityTable + `' ('data') VALUES (?)`)
		if err != nil {
			return key, err
		}
		defer stmt.Close()

		result, err := stmt.Exec(buffer.Bytes())
		if err != nil {
			return key, err
		}

		id, err := result.LastInsertId()
		if err != nil {
			return key, err
		}

		nkey := Key{id}
		key = &nkey
	} else {
		// update data
		stmt, err := tx.Prepare(`REPLACE INTO '` + EntityTable + `' ('data', 'id') VALUES (?, ?)`)
		if err != nil {
			return key, err
		}
		defer stmt.Close()

		if _, err := stmt.Exec(buffer.Bytes(), key.int64); err != nil {
			return key, err
		}
	}

	// insert/update indices
	if err := d.createIndices(key, src, tx); err != nil {
		return key, err
	}

	tx.Commit()
	return key, nil
}

func (d *Datastore) PutMulti(keys []*Key, srcs []interface{}, breakOnError bool) ([]*Key, error) {
	if len(keys) != len(srcs) {
		return keys, errors.New("keys and source slices must have equal length")
	}

	var e error
	var err error
	var nkeys []*Key

	for i, key := range keys {
		nkeys[i], err = d.Put(key, srcs[i])
		if err != nil {
			if breakOnError {
				return nkeys, err
			}
			e = err
		}
	}

	return nkeys, e
}

func (d *Datastore) createIndices(key *Key, e interface{}, tx *sql.Tx) error {
	v := reflect.ValueOf(e)
	codec, err := d.getStructCodec(v)
	if err != nil {
		return err
	}

	for fieldname, _ /*fieldtype*/ := range codec {
		fieldvalue := v.FieldByName(fieldname)
		//log.Printf("indexing: %v => %v\t%v\n", fieldname, fieldtype, fieldvalue)

		stmt, err := tx.Prepare(`REPLACE INTO '` + IndexPrefix + `_` + fieldname + `' ('entitiy_id', 'value') VALUES (?, ?)`)
		if err != nil {
			return err
		}

		switch vi := fieldvalue.Interface().(type) {
		default:
			//log.Println(vi)
			if _, err := stmt.Exec(key.int64, vi); err != nil {
				stmt.Close()
				return err
			}
		}

		stmt.Close()
	}

	return nil
}

func (d *Datastore) getStructCodec(v reflect.Value) (map[string]string, error) {
	t := v.Type()

	d.structure.RLock()
	defer d.structure.RUnlock()

	if codec, ok := d.structure.codec[t]; ok {
		return codec, nil
	}

	return nil, errors.New("schemalessql: unknown entity type")
}

func (d *Datastore) Get(key *Key, dst interface{}) error {
	if key == nil {
		return sql.ErrNoRows
	}

	if err := d.Register(dst); err != nil {
		return err
	}

	// fetch gob encoded data
	stmt, err := d.Prepare(`SELECT data FROM '` + EntityTable + `' WHERE id=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var data string
	if err := stmt.QueryRow(key.int64).Scan(&data); err != nil {
		return err
	}

	// decode data
	dec := gob.NewDecoder(bytes.NewBufferString(data))
	if err := dec.Decode(dst); err != nil {
		return err
	}

	return nil
}

func (d *Datastore) GetMulti(keys []*Key, dsts []interface{}, breakOnError bool) error {
	if len(keys) != len(dsts) {
		return errors.New("keys and destination slices must have equal length")
	}

	var e error

	for i, key := range keys {
		err := d.Get(key, dsts[i])
		if err != nil {
			if breakOnError {
				return err
			}
			e = err
		}
	}

	return e
}

func (d *Datastore) Delete(key *Key) error {
	if key == nil {
		return sql.ErrNoRows
	}

	tx, err := d.Begin()
	if err != nil {
		return ErrCouldNotSetup
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM '` + EntityTable + `' WHERE id=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(key.int64); err != nil {
		return err
	}

	d.structure.RLock()
	defer d.structure.RUnlock()

	// structure.codec = map[reflect.Type]map[string]string
	for _, codec := range d.structure.codec {
		for fieldname, _ := range codec {

			stmt, err := tx.Prepare(`DELETE FROM '` + IndexPrefix + `_` + fieldname + `' WHERE entitiy_id=?`)
			if err != nil {
				return err
			}
			defer stmt.Close()

			if _, err := stmt.Exec(key.int64); err != nil {
				return err
			}

		}
	}

	tx.Commit()
	return nil
}

func (d *Datastore) DeleteMulti(keys []*Key, breakOnError bool) error {
	var e error

	for _, key := range keys {
		err := d.Delete(key)
		if err != nil {
			if breakOnError {
				return err
			}
			e = err
		}
	}

	return e
}

// TODO

func (d *Datastore) Find(query map[string]interface{}, dsts []interface{}) error {
	return errors.New("schemalessql: not yet implemented")
}

func (d *Datastore) FindOne(query map[string]interface{}, dst interface{}) error {
	return errors.New("schemalessql: not yet implemented")
}