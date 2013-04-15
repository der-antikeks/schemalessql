package schemalessql_test

import (
	"github.com/der-antikeks/schemalessql"
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

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3)}
	_, err := db.Put(nil, e)

	if err != nil {
		t.Fatalf("error creating entity: %v", err)
	}
}

// TODO

func TestRead(t *testing.T)   {}
func TestUpdate(t *testing.T) {}
func TestDelete(t *testing.T) {}

func TestCreateMulti(t *testing.T) {}
func TestReadMulti(t *testing.T)   {}
func TestUpdateMulti(t *testing.T) {}
func TestDeleteMulti(t *testing.T) {}

func TestQuery(t *testing.T)      {}
func TestQueryMulti(t *testing.T) {}
