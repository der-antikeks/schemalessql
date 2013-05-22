/*
Schemaless SQL

Example:

	db, _ := schemalessql.Open("sqlite3", "./foo.db")
	defer db.Close()

	type Entity struct {
		Value      string
		Changed    time.Time
		LastLoaded time.Time
		LastSaved  time.Time
	}

	func (e *Entity) BeforeSave() {
		e.LastSaved = time.Now()
	}

	func (e *Entity) AfterLoad() {
		e.LastLoaded = time.Now()
	}

	// create
	e := Entity{"data", time.Now()}
	key, err := db.Put(nil, e)

	// update
	e.Value = "updated data"
	key, err := db.Put(key, e)

	// read
	var r Entity
	err := db.Get(key, &r)

	// delete
	err := db.Delete(key)

	// multi ops
	entities := []Entity{
		Entity{"A", time.Now()},
		Entity{"B", time.Now()},
	}
	keys, err := db.PutMulti(nil, entities, true)

	results := make([]Entity, len(keys))
	err := db.GetMulti(keys, results, true)

	err := db.DeleteMulti(keys, true)

	// query
	q := map[string]interface{}{
		"A": 123,
		"C": true,
	}

	q, err = db.Query(q)
	if err != nil {
		t.Fatalf("error finding entity: %v", err)
	}

	for {
		var r Entity
		key, err := q.Next(&r)

		if err == schemalessql.Done {
			break
		}
		if err != nil {
			t.Fatalf("error finding entity: %v", err)
		}
	}

*/
package schemalessql
