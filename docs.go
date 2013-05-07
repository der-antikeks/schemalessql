/*
Schemaless SQL

Example:

	db := schemalessql.Open("sqlite3", "./foo.db")
	defer db.Close()

	type Entity struct {
		Value   string
		Changed time.Time
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
	entities := []Entity{Entity{"A", time.Now()}, Entity{"B", time.Now()}}
	keys, err := db.PutMulti(nil, entities, true)
	results := make([]Entity, len(keys))
	err := db.GetMulti(keys, results, true)
	err := db.DeleteMulti(keys, true)

	// query
	err := db.Find(query map[string]interface{}, dsts []interface{})
	err := db.FindOne(query map[string]interface{}, dst interface{})

*/
package schemalessql
