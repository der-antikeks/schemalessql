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

// TODO

func TestCreateMulti(t *testing.T) {}
func TestReadMulti(t *testing.T)   {}
func TestUpdateMulti(t *testing.T) {}
func TestDeleteMulti(t *testing.T) {}

func TestQuery(t *testing.T)      {}
func TestQueryMulti(t *testing.T) {}
