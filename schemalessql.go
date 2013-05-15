package schemalessql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// Table in which the gob encoded data is stored.
var EntityTable = "entities"

// Prefix for tables in which the indices are stored.
// ("IndexPrefix"_fieldname)
var IndexPrefix = "index"

// Datatstore contains the database handle and controls the creation of necessary tables.
type Datastore struct {
	*sql.DB
	structure struct {
		sync.RWMutex
		created map[reflect.Type]bool
		codec   map[string]string
	}
}

// Open opens a database specified by its database driver name and a driver-specific data source name, usually consisting of at least a database name and connection information.
func Open(driverName, dataSourceName string) (*Datastore, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	d := Datastore{DB: db}
	d.structure.created = make(map[reflect.Type]bool)
	d.structure.codec = make(map[string]string)
	return &d, nil
}

// Register creates entitiy and index tables with suitable types.
func (d *Datastore) Register(src interface{}) error {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	// check if already registered
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
		return fmt.Errorf("schemalessql: required tables/indices could not be created: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS '` + EntityTable + `' ('id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 'data' BLOB NOT NULL)`); err != nil {
		return fmt.Errorf("schemalessql: required tables/indices could not be created: %v", err)
	}

	if _, err := tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS 'id_index' ON '` + EntityTable + `' ('id' ASC)`); err != nil {
		return fmt.Errorf("schemalessql: required tables/indices could not be created: %v", err)
	}

	// create index tables for registered reflect.Type
	n := t.NumField()
	for i := 0; i < n; i++ {
		vt := t.Field(i)
		vf := v.Field(i)

		// register type for gob
		if vf.CanInterface() && vf.Interface() != nil {
			gob.Register(vf.Interface())
		}

		if vt.Tag.Get("datastore") == "noindex" {
			continue
		}

		var fieldtype string

		switch vf.Interface().(type) {
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
				return fmt.Errorf("schemalessql: unsupported struct field type: %v", vf.Kind())
			}
		}

		fieldname := vt.Name
		tmptype, found := d.structure.codec[fieldname]

		if found && tmptype != fieldtype {
			// fieldname already used, with wrong type
			return fmt.Errorf("schemalessql: could not register entity %v, field %v already registered as %v instead of %v", t, fieldname, tmptype, fieldtype)
		}

		// new field
		if !found {
			d.structure.codec[fieldname] = fieldtype

			if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS '` + IndexPrefix + `_` + fieldname + `' ('entitiy_id' INTEGER NOT NULL UNIQUE, 'value' ` + fieldtype + `)`); err != nil {
				return fmt.Errorf("schemalessql: required tables/indices could not be created: %v", err)
			}

			if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS 'id_value_index' ON '` + IndexPrefix + `_` + fieldname + `' ('entitiy_id' ASC, 'value' ASC)`); err != nil {
				return fmt.Errorf("schemalessql: required tables/indices could not be created: %v", err)
			}
		}
	}

	tx.Commit()
	d.structure.created[t] = true
	return nil
}

// Key is the primary key of a saved Entity
type Key struct {
	int64
}

// The BeforeSave() method of an entity that satisfies schemalessql.BeforeSaver is called before saving to database.
type BeforeSaver interface {
	BeforeSave()
}

// The AfterSave() method of an entity that satisfies schemalessql.AfterSaver is called after saving to database.
type AfterSaver interface {
	AfterSave()
}

// Put saves the provided entity gob-encoded into the database and updates the corresponding index tables.
// An existing entity and its indices will be updated if a non-nil Key is passed.
// The Key of the updated or created database entry is returned.
func (d *Datastore) Put(key *Key, src interface{}) (*Key, error) {
	if bs, ok := src.(BeforeSaver); ok {
		bs.BeforeSave()
	}

	if err := d.Register(src); err != nil {
		return key, err
	}

	// encode data
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(src); err != nil {
		return key, fmt.Errorf("schemalessql: could not encode entity: %v", err)
	}

	// begin transaction
	tx, err := d.Begin()
	if err != nil {
		return key, fmt.Errorf("schemalessql: could not insert data into db: %v", err)
	}
	defer tx.Rollback()

	if key == nil {
		// insert data
		stmt, err := tx.Prepare(`INSERT INTO '` + EntityTable + `' ('data') VALUES (?)`)
		if err != nil {
			return key, fmt.Errorf("schemalessql: could not insert data into db: %v", err)
		}
		defer stmt.Close()

		result, err := stmt.Exec(buffer.Bytes())
		if err != nil {
			return key, fmt.Errorf("schemalessql: could not insert data into db: %v", err)
		}

		id, err := result.LastInsertId()
		if err != nil {
			return key, fmt.Errorf("schemalessql: could not insert data into db: %v", err)
		}

		nkey := Key{id}
		key = &nkey
	} else {
		// update data
		stmt, err := tx.Prepare(`REPLACE INTO '` + EntityTable + `' ('data', 'id') VALUES (?, ?)`)
		if err != nil {
			return key, fmt.Errorf("schemalessql: could not insert data into db: %v", err)
		}
		defer stmt.Close()

		if _, err := stmt.Exec(buffer.Bytes(), key.int64); err != nil {
			return key, fmt.Errorf("schemalessql: could not insert data into db: %v", err)
		}
	}

	// insert/update indices
	if err := d.createIndices(key, src, tx); err != nil {
		return key, err
	}

	tx.Commit()

	if as, ok := src.(AfterSaver); ok {
		as.AfterSave()
	}

	return key, nil
}

// PutMulti is identical to Put, except that it takes multiple entities and keys.
// If breakOnError is true the method will return as soon as an error occurs.
func (d *Datastore) PutMulti(keys []*Key, srcs interface{}, breakOnError bool) ([]*Key, error) {
	vsrcs := reflect.ValueOf(srcs)
	if vsrcs.Kind() != reflect.Slice {
		return keys, fmt.Errorf("schemalessql: source must be a slice")
	}

	if keys == nil {
		keys = make([]*Key, vsrcs.Len())
	} else if len(keys) != vsrcs.Len() {
		return keys, fmt.Errorf("schemalessql: keys and source slices must have equal length")
	}

	var e error
	var err error
	nkeys := make([]*Key, vsrcs.Len())

	for i, key := range keys {
		nkeys[i], err = d.Put(key, vsrcs.Index(i).Interface())
		if err != nil {
			if breakOnError {
				return nkeys, err
			}
			e = err
		}
	}

	return nkeys, e
}

// createIndices inserts new data into the index tables.
func (d *Datastore) createIndices(key *Key, e interface{}, tx *sql.Tx) error {
	v := reflect.ValueOf(e)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	codec, err := d.getStructCodec(v)
	if err != nil {
		return err
	}

	for fieldname, _ := range codec {
		fieldvalue := v.FieldByName(fieldname)

		stmt, err := tx.Prepare(`REPLACE INTO '` + IndexPrefix + `_` + fieldname + `' ('entitiy_id', 'value') VALUES (?, ?)`)
		if err != nil {
			return fmt.Errorf("schemalessql: could not insert data into db: %v", err)
		}

		// TODO: is this necessary?
		switch vi := fieldvalue.Interface().(type) {
		default:
			if _, err := stmt.Exec(key.int64, vi); err != nil {
				stmt.Close()
				return fmt.Errorf("schemalessql: could not insert data into db: %v", err)
			}
		}

		stmt.Close()
	}

	return nil
}

// getStructCodec returns the structure of the provided value if it has been registered before.
func (d *Datastore) getStructCodec(v reflect.Value) (map[string]string, error) {
	t := v.Type()

	d.structure.RLock()
	defer d.structure.RUnlock()

	// TODO: return only codec of type
	if d.structure.created[t] {
		return d.structure.codec, nil
	}

	return nil, fmt.Errorf("schemalessql: unknown entity type %v", t)
}

// The BeforeLoad() method of an entity that satisfies schemalessql.BeforeLoader is called before it will be filled with data.
type BeforeLoader interface {
	BeforeLoad()
}

// The AfterLoad() method of an entity that satisfies schemalessql.AfterLoader is called after it was filled with data.
type AfterLoader interface {
	AfterLoad()
}

// Get fetches an entity with the Key and gob-decodes it into the provided interface.
// If no entry is found for this Key, sql.ErrNoRows is returned.
func (d *Datastore) Get(key *Key, dst interface{}) error {
	if key == nil {
		return sql.ErrNoRows
	}

	if bl, ok := dst.(BeforeLoader); ok {
		bl.BeforeLoad()
	}

	if err := d.Register(dst); err != nil {
		return err
	}

	// fetch gob encoded data
	stmt, err := d.Prepare(`SELECT data FROM '` + EntityTable + `' WHERE id=?`)
	if err != nil {
		return fmt.Errorf("schemalessql: could not query data from db: %v", err)
	}
	defer stmt.Close()

	var data string
	if err := stmt.QueryRow(key.int64).Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return err
		}

		return fmt.Errorf("schemalessql: could not query data from db: %v", err)
	}

	// decode data
	dec := gob.NewDecoder(bytes.NewBufferString(data))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("schemalessql: could not decode entity: %v", err)
	}

	if al, ok := dst.(AfterLoader); ok {
		al.AfterLoad()
	}

	return nil
}

// GetMulti is identical to Get, except that it takes multiple keys.
// If breakOnError is true the method will return as soon as an error occurs.
func (d *Datastore) GetMulti(keys []*Key, dsts interface{}, breakOnError bool) error {
	vdsts := reflect.ValueOf(dsts)
	if vdsts.Kind() != reflect.Slice {
		return fmt.Errorf("schemalessql: destination must be a slice")
	}

	if len(keys) != vdsts.Len() {
		return fmt.Errorf("schemalessql: keys and destination slices must have equal length")
	}

	var e error
	for i, key := range keys {

		// TODO: wtf am i doing here?
		switch vi := vdsts.Index(i).Addr().Interface().(type) {
		default:
			err := d.Get(key, vi)
			if err != nil {
				if breakOnError {
					return err
				}
				e = err
			}
		}

	}

	return e
}

// Delete removes the entity of the provided Key and its indices from the database.
// If no entry is found for this Key, sql.ErrNoRows is returned.
func (d *Datastore) Delete(key *Key) error {
	if key == nil {
		return sql.ErrNoRows
	}

	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("schemalessql: could not delete data from db: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM '` + EntityTable + `' WHERE id=?`)
	if err != nil {
		return fmt.Errorf("schemalessql: could not delete data from db: %v", err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(key.int64); err != nil {
		return fmt.Errorf("schemalessql: could not delete data from db: %v", err)
	}

	d.structure.RLock()
	defer d.structure.RUnlock()

	// structure.codec = map[string]string
	for fieldname, _ := range d.structure.codec {

		stmt, err := tx.Prepare(`DELETE FROM '` + IndexPrefix + `_` + fieldname + `' WHERE entitiy_id=?`)
		if err != nil {
			return fmt.Errorf("schemalessql: could not delete data from db: %v", err)
		}
		defer stmt.Close()

		if _, err := stmt.Exec(key.int64); err != nil {
			return fmt.Errorf("schemalessql: could not delete data from db: %v", err)
		}

	}

	tx.Commit()
	return nil
}

// DeleteMulti is identical to Delete, except that it takes multiple keys.
// If breakOnError is true the method will return as soon as an error occurs.
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

// FindKeys searches indexed fields for all entries that match the filter criteria and returns its keys.
// If no entry is found, sql.ErrNoRows is returned.
func (d *Datastore) FindKeys(query map[string]interface{}) ([]*Key, error) {
	tmp := make(map[int64]int)

	for fieldname, value := range query {
		// TODO: is this safe for concurrent use? I bet not
		if _, found := d.structure.codec[fieldname]; !found {
			//continue
			return nil, sql.ErrNoRows
		}

		stmt, err := d.Prepare(`SELECT entitiy_id FROM '` + IndexPrefix + `_` + fieldname + `' WHERE value=?`)
		if err != nil {
			return nil, fmt.Errorf("schemalessql: could not query data from db: %v", err)
		}
		defer stmt.Close()

		rows, err := stmt.Query(value)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, err
			}

			return nil, fmt.Errorf("schemalessql: could not query data from db: %v", err)
		}

		for rows.Next() {
			var id int64
			rows.Scan(&id)
			tmp[id]++
		}
	}

	l := len(query)
	var result []*Key

	for i, n := range tmp {
		if n == l {
			result = append(result, &Key{i})
		}
	}

	return result, nil
}

// Find searches indexed fields for all entries that match the filter criteria and returns these as a slice of the provided interface.
// If no entry is found, sql.ErrNoRows is returned.
func (d *Datastore) Find(query map[string]interface{}, destype interface{}) ([]interface{}, error) {
	vdestype := reflect.ValueOf(destype)
	if vdestype.Kind() != reflect.Struct {
		return nil, fmt.Errorf("schemalessql: destination type must be a struct")
	}

	keys, err := d.FindKeys(query)
	if err != nil {
		return nil, err
	}

	var dsts []interface{}
	for _, key := range keys {

		// I must admit that I have no idea why I can not directly pass the value instead of the pointer
		// TODO: panic: reflect: NumField of non-struct type @ Register() :84
		//e := reflect.Zero(vdestype.Type()).Interface()
		e := reflect.New(vdestype.Type()).Interface()
		err := d.Get(key, e /* & */)
		if err != nil {
			return nil, err
		}

		//dsts = append(dsts, e)
		dsts = append(dsts, reflect.ValueOf(e).Elem().Interface())
	}

	return dsts, nil
}

// FindOne is identical to Find, except that it returns only one entity.
func (d *Datastore) FindOne(query map[string]interface{}, dst interface{}) error {
	keys, err := d.FindKeys(query)
	if err != nil {
		return err
	}

	if len(keys) < 1 {
		return sql.ErrNoRows
	}

	if err := d.Get(keys[0], dst); err != nil {
		return err
	}

	return nil
}
