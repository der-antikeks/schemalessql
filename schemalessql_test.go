package schemalessql_test

import (
	"database/sql"
	"github.com/der-antikeks/schemalessql"
	"reflect"
	"testing"
	"time"

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

func TestRegisterStruct(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	a := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	if err := db.Register(a); err != nil {
		t.Fatalf("error registering entity: %v", err)
	}
}

func TestRegisterPointer(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	var a Entity
	if err := db.Register(&a); err != nil {
		t.Fatalf("error registering entity: %v", err)
	}
}

type EntityA struct {
	Data float64
}

type EntityB struct {
	Data string
}

func TestRegisterDuplicate(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	var a EntityA
	if err := db.Register(&a); err != nil {
		t.Fatalf("error registering entity a: %v", err)
	}

	var b EntityB
	if err := db.Register(&b); err == nil {
		t.Fatalf("should receive error while registering entity b but got: %v", err)
	}
}

func TestCreate(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	key := schemalessql.NewIncompleteKey("Foo")
	if _, err := db.Put(key, e); err != nil {
		t.Fatalf("error creating entity: %v", err)
	}
}

func TestRead(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute}
	key := schemalessql.NewIncompleteKey("Foo")
	key, err := db.Put(key, e)
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
	key := schemalessql.NewIncompleteKey("Foo")
	key, err := db.Put(key, e)
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
	key := schemalessql.NewIncompleteKey("Foo")
	key, err := db.Put(key, e)
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

	keys := []*schemalessql.Key{
		schemalessql.NewIncompleteKey("Foo"),
		schemalessql.NewIncompleteKey("Foo"),
	}

	if _, err := db.PutMulti(keys, entities, true); err != nil {
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

	keys := []*schemalessql.Key{
		schemalessql.NewIncompleteKey("Foo"),
		schemalessql.NewIncompleteKey("Foo"),
	}

	keys, err := db.PutMulti(keys, entities, true)
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

	keys := []*schemalessql.Key{
		schemalessql.NewIncompleteKey("Foo"),
		schemalessql.NewIncompleteKey("Foo"),
	}

	keys, err := db.PutMulti(keys, entities, true)
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

	keys := []*schemalessql.Key{
		schemalessql.NewIncompleteKey("Foo"),
		schemalessql.NewIncompleteKey("Foo"),
	}

	keys, err := db.PutMulti(keys, entities, true)
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

type EntityCreateHook struct {
	Data      string
	LastSaved time.Time
	Saved     bool
}

func (e *EntityCreateHook) BeforeSave() {
	e.LastSaved = time.Now()
}

func (e *EntityCreateHook) AfterSave() {
	e.Saved = true
}

func TestCreateHooks(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := EntityCreateHook{Data: "A"}
	key := schemalessql.NewIncompleteKey("Foo")
	key, err := db.Put(key, &e)
	if err != nil {
		t.Fatalf("error creating entity: %v", err)
	}

	var r EntityCreateHook
	if err := db.Get(key, &r); err != nil {
		t.Fatalf("error reading entity: %v", err)
	}

	if e.Data != r.Data || e.LastSaved != r.LastSaved || e.Saved == r.Saved {
		t.Fatalf("entities do not match: \n%v\n%v", e, r)
	}
}

type EntityReadHook struct {
	Data       string
	LastLoaded time.Time
	Loaded     bool
	Test       bool
}

func (e *EntityReadHook) BeforeLoad() {
	e.Test = true
}

func (e *EntityReadHook) AfterLoad() {
	e.LastLoaded = time.Now()
	e.Loaded = true
}

func TestReadHooks(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	e := EntityReadHook{Data: "A"}
	key := schemalessql.NewIncompleteKey("Foo")
	key, err := db.Put(key, &e)
	if err != nil {
		t.Fatalf("error creating entity: %v", err)
	}

	var r EntityReadHook
	if err := db.Get(key, &r); err != nil {
		t.Fatalf("error reading entity: %v", err)
	}

	if e.Data != r.Data || e.LastLoaded == r.LastLoaded || e.Loaded == r.Loaded || e.Test == r.Test {
		t.Fatalf("entities do not match: \n%v\n%v", e, r)
	}
}

func TestQueryMulti(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	entities := []interface{}{
		Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute},
		Entity{456, 456.789, true, []byte{21, 43, 65}, "bar", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), time.Duration(10) * time.Second},
	}

	keys := []*schemalessql.Key{
		schemalessql.NewIncompleteKey("Foo"),
		schemalessql.NewIncompleteKey("Foo"),
	}

	if _, err := db.PutMulti(keys, entities, true); err != nil {
		t.Fatalf("error creating entities: %v", err)
	}

	// find one
	query := map[string]interface{}{
		"A": 123,
		"C": true,
	}

	results, err := db.Find(query, Entity{})
	if err != nil {
		t.Fatalf("error finding entities: %v", err)
	}

	if n := len(results); n != 1 {
		t.Fatalf("error finding entities, number of results: %v", n)
	}

	if !reflect.DeepEqual(entities[0], results[0]) {
		t.Fatalf("error finding entities, result does not match: \n%v\n%v", entities[0], results[0])
	}

	// find two
	results, err = db.Find(map[string]interface{}{"C": true}, Entity{})
	if err != nil {
		t.Fatalf("error finding entities: %v", err)
	}

	if n := len(results); n != 2 {
		t.Fatalf("error finding entities, number of results: %v", n)
	}

	if !reflect.DeepEqual(entities[0], results[0]) || !reflect.DeepEqual(entities[1], results[1]) {
		t.Fatalf("error finding entities, result does not match: \n%v\n%v", entities[0], results[1])
	}

	// find nothing
	results, err = db.Find(map[string]interface{}{"C": false}, Entity{})
	if err != nil {
		t.Fatalf("error finding entities: %v", err)
	}

	if n := len(results); n != 0 {
		t.Fatalf("error finding entities, number of results: %v", n)
	}

}

func TestQuery(t *testing.T) {
	db := newDB(t)
	defer closeDB(t, db)

	entities := []interface{}{
		Entity{123, 123.456, true, []byte{12, 34, 56}, "foo", time.Now(), time.Duration(3) * time.Minute},
		Entity{456, 456.789, true, []byte{21, 43, 65}, "bar", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), time.Duration(10) * time.Second},
	}

	keys := []*schemalessql.Key{
		schemalessql.NewIncompleteKey("Foo"),
		schemalessql.NewIncompleteKey("Foo"),
	}

	if _, err := db.PutMulti(keys, entities, true); err != nil {
		t.Fatalf("error creating entities: %v", err)
	}

	// find one
	query := map[string]interface{}{
		"A": 456,
		"C": true,
	}

	var r Entity
	if err := db.FindOne(query, &r); err != nil {
		t.Fatalf("error finding entity: %v", err)
	}

	if !reflect.DeepEqual(entities[1], r) {
		t.Fatalf("error finding entity, result does not match: \n%v\n%v", entities[0], r)
	}

	// find nothing
	if err := db.FindOne(map[string]interface{}{"C": false}, &r); err != sql.ErrNoRows {
		t.Fatalf("error finding entity: %v", err)
	}

}
