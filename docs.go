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
	keys, err := db.PutMulti([]keys, []entities)
	err := db.GetMulti([]keys, []&entities)
	err := db.DeleteMulti([]keys)

	// query
	err := db.Find(query map[string]interface{}, dsts []interface{})
	err := db.FindOne(query map[string]interface{}, dst interface{})

*/
package schemalessql