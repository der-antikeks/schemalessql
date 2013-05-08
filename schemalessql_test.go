package schemalessql_test

import (
	"database/sql"
	"github.com/der-antikeks/schemalessql"
	"reflect"
	"testing"
	"time"

	// TODO: use fakedb for testing?
	_ "github.com/mattn/go-sqlite3"
)

type Entity struct {
	A int64
	B float64
	C bool
	D []byte
	E string
	F time.Time

	IgnoreMe interface{} `datastore:"noindex"`
}

func newDB(t *testing.T) *schemalessql.Datastore {
	db, err := schemalessql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("error connecting to database: %v", err)
	}

	return db
}

func closeDB(t *testing.T, db *schemalessql.Datastore) {
	if err := db.Close(); err != nil {
		t.Fatalf("error closing datastore: %v", err)
	}
}

func TestRegister1(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	a := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	if err := db.Register(a); err != nil {
		t.Fatalf("error registering entity: %v", err)
	}
}

func TestRegister2(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	var a Entity
	if err := db.Register(&a); err != nil {
		t.Fatalf("error registering entity: %v", err)
	}
}

func TestCreate(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	if _, err := db.Put(nil, e); err != nil {
		t.Fatalf("error creating entity: %v", err)
	}
}

func TestRead(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	key, err := db.Put(nil, e)
	if err != nil {
		t.Fatalf("error creating entity: %v", err)
	}

	var r Entity
	if err := db.Get(key, &r); err != nil {
		t.Fatalf("error reading entity: %v", err)
	}

	if !reflect.DeepEqual(e, r) {
		t.Fatalf("entities do not match: \n%v\n%v", e, r)
	}
}

func TestUpdate(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	key, err := db.Put(nil, e)
	if err != nil {
		t.Fatalf("error creating entity: %v", err)
	}

	u := e
	u.E = "updated data"
	u.F = time.Now().Add(e.IgnoreMe.(time.Duration))
	if _, err := db.Put(key, u); err != nil {
		t.Fatalf("error updating entity: %v", err)
	}

	var r Entity
	if err := db.Get(key, &r); err != nil {
		t.Fatalf("error reading entity: %v", err)
	}

	if reflect.DeepEqual(e, u) || !reflect.DeepEqual(u, r) {
		t.Fatalf("entities do not match: \n%v\n%v", u, r)
	}
}

func TestDelete(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	key, err := db.Put(nil, e)
	if err != nil {
		t.Fatalf("error creating entity: %v", err)
	}

	if err := db.Delete(key); err != nil {
		t.Fatalf("error deleting entity: %v", err)
	}

	var r Entity
	if err := db.Get(key, &r); err != sql.ErrNoRows {
		t.Fatalf("failed to delete entity: %v", err)
	}
}

func TestCreateMulti(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	entities := []interface{}{
		Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute},
		Entity{456, 456.789, false, []byte{21, 43, 65}, "bar", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), time.Duration(10) * time.Second},
	}

	if _, err := db.PutMulti(nil, entities, true); err != nil {
		t.Fatalf("error creating entities: %v", err)
	}
}

func TestReadMulti(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	entities := []Entity{
		Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute},
		Entity{456, 456.789, false, []byte{21, 43, 65}, "bar", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), time.Duration(10) * time.Second},
	}

	keys, err := db.PutMulti(nil, entities, true)
	if err != nil {
		t.Fatalf("error creating entities: %v", err)
	}

	results := make([]Entity, len(keys))
	if err := db.GetMulti(keys, results, true); err != nil {
		t.Fatalf("error reading entities: %v", err)
	}

	for i, e := range entities {
		if r := results[i]; !reflect.DeepEqual(e, r) {
			t.Fatalf("entities do not match: \n%v\n%v", e, r)
		}
	}
}

func TestUpdateMulti(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	entities := []Entity{
		Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute},
		Entity{456, 456.789, false, []byte{21, 43, 65}, "bar", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), time.Duration(10) * time.Second},
	}

	keys, err := db.PutMulti(nil, entities, true)
	if err != nil {
		t.Fatalf("error creating entities: %v", err)
	}

	updated := make([]Entity, len(entities))
	copy(updated, entities)

	updated[0].E = "updated data"
	updated[0].F = time.Now().Add(updated[0].IgnoreMe.(time.Duration))
	updated[1].E = "updated data2"
	updated[1].F = time.Now().Add(updated[1].IgnoreMe.(time.Duration))
	if _, err := db.PutMulti(keys, updated, true); err != nil {
		t.Fatalf("error updating entity: %v", err)
	}

	results := make([]Entity, len(keys))
	if err := db.GetMulti(keys, results, true); err != nil {
		t.Fatalf("error reading entities: %v", err)
	}

	for i, u := range updated {
		r := results[i]
		if reflect.DeepEqual(entities[i], u) || !reflect.DeepEqual(u, r) {
			t.Fatalf("entities do not match: \n%v\n%v", u, r)
		}
	}

}

func TestDeleteMulti(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	entities := []Entity{
		Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute},
		Entity{456, 456.789, false, []byte{21, 43, 65}, "bar", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), time.Duration(10) * time.Second},
	}

	keys, err := db.PutMulti(nil, entities, true)
	if err != nil {
		t.Fatalf("error creating entities: %v", err)
	}

	if err := db.DeleteMulti(keys, true); err != nil {
		t.Fatalf("error deleting entities: %v", err)
	}

	results := make([]Entity, len(keys))
	if err := db.GetMulti(keys, results, true); err != sql.ErrNoRows {
		t.Fatalf("failed to delete entities: %v", err)
	}
}

// TODO

func TestQuery(t *testing.T)      {}
func TestQueryMulti(t *testing.T) {}
