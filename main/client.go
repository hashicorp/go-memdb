package main

import (
	"github.com/manhdaovan/go-memdb"
	"github.com/manhdaovan/go-memdb/explorer_server"
)

// Create a sample struct
type Person struct {
	Email string
	Name  string
	Age   int
}



func main() {
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
		},
	}
	// Create a new data base
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}

	// Create a write transaction
	txn := db.Txn(true)

	// Insert a new person
	p := &Person{"joe@aol.com", "Joe", 30}
	if err := txn.Insert("person", p); err != nil {
		panic(err)
	}

	// Commit the transaction
	txn.Commit()

	memdb.InitGlobalExplorer(db.Txn(false))

	sv := explorer_server.NewServer()
	sv.Run(":8888")
}