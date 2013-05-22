# Schemaless SQL

__Usage:__

	package main

	import (
		"github.com/der-antikeks/schemalessql"
	)

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

	func main() {
		db, _ := schemalessql.Open("sqlite3", "./foo.db")
		defer db.Close()

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
	}
