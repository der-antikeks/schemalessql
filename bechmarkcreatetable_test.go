package schemalessql_test

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"sync"
	"testing"
)

var sqls = []string{
	"CREATE TABLE IF NOT EXISTS 'index_dimension' ('dimension_id' TEXT, 'entity_id' INTEGER)",
	"CREATE UNIQUE INDEX IF NOT EXISTS 'dimension_entity_index' ON 'index_dimension' ('dimension_id', 'entity_id' ASC)",
}

func BenchmarkCreateTable(b *testing.B) {
	b.StopTimer()

	os.Remove("./foo.db")

	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		b.Fatal("error connecting:", err)
	}
	defer db.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			if _, err = db.Exec(sql); err != nil {
				b.Fatal("error executing:", err, sql)
			}
		}
	}
}

func BenchmarkCreateTablePrecreate(b *testing.B) {
	b.StopTimer()

	os.Remove("./foo.db")

	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		b.Fatal("error connecting:", err)
	}
	defer db.Close()

	for _, sql := range sqls {
		if _, err = db.Exec(sql); err != nil {
			b.Fatal("error executing:", err, sql)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			if _, err = db.Exec(sql); err != nil {
				b.Fatal("error executing:", err, sql)
			}
		}
	}
}

func BenchmarkCreateTableMaptest(b *testing.B) {
	b.StopTimer()

	os.Remove("./foo.db")

	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		b.Fatal("error connecting:", err)
	}
	defer db.Close()

	var maptest = struct {
		sync.RWMutex
		m map[string]bool
	}{m: make(map[string]bool)}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {

			maptest.RLock()
			found := maptest.m[sql]
			maptest.RUnlock()

			if !found {
				maptest.Lock()
				maptest.m[sql] = true
				maptest.Unlock()

				if _, err = db.Exec(sql); err != nil {
					b.Fatal("error executing:", err, sql)
				}
			}
		}
	}
}
