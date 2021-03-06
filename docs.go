/*
Schemaless SQL

Example:

	db := schemalessql.Open("sqlite3", "./foo.db")
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
	query := map[string]interface{}{
		"A": 123,
		"C": true,
	}
	results, err := db.Find(query, Entity{})

	var r Entity
	err := db.FindOne(query, &r)

*/
package schemalessql
