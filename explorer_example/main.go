package main

import (
	"github.com/manhdaovan/go-memdb"
	"github.com/manhdaovan/go-memdb/explorer_server"
	"strconv"
)

// Create a sample struct
type Person struct {
	Email string
	Name  string
	Age   int
}

func main() {
	// Somewhere in your application
	// Create the DB schema
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"person": &memdb.TableSchema{
				Name: "person",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Email"},
					},
				},
			},
			"blog": &memdb.TableSchema{
				Name: "blog",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Email"},
					},
				},
			},
			"comment": &memdb.TableSchema{
				Name: "comment",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Email"},
					},
				},
			},
		},
	}
	// Create a new data base
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}

	// Create a write transaction
	txn := db.Txn(true)

	// Insert new 100 persons
	var p *Person
	var iStr string
	for idx := int64(0); idx < 100; idx++ {
		iStr = strconv.FormatInt(idx, 10)
		p = &Person{"joe-"+ iStr +"@aol.com", "Joe" + iStr, int(idx)}
		if err := txn.Insert("person", p); err != nil {
			panic(err)
		}
	}

	// Commit the transaction
	txn.Commit()

	// And explorer starts here
	explorer := memdb.NewExplorer(db.Txn(false))
	sv := explorer_server.NewServer(explorer, "../explorer_server/assets", "../explorer_server/templates/*")
	sv.Run(":8888")
}