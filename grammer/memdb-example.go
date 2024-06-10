package main

import (
	"fmt"
	"log"

	"github.com/hashicorp/go-memdb"
)

func main() {
	type entry struct {
		Address string
		Version string
	}
	db, err := memdb.NewMemDB(&memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"providers": {
				Name: "providers",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:   "id",
						Unique: true,
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.StringFieldIndex{Field: "Address"},
								&memdb.StringFieldIndex{Field: "Version"},
							},
							AllowMissing: true,
						},
					},
					"provider": {
						Name: "provider",
						Indexer: &memdb.CompoundMultiIndex{
							Indexes: []memdb.Indexer{
								&memdb.StringFieldIndex{Field: "Address"},
								&memdb.StringFieldIndex{Field: "Version"},
							},
							AllowMissing: true,
						},
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	wTxn := db.Txn(true)
	defer wTxn.Abort()
	err = wTxn.Insert("providers", &entry{
		Address: "aws",
		Version: "1.0.0",
	})
	err = wTxn.Insert("providers", &entry{
		//Address: "awt",
		Version: "1.0.1",
	})
	if err != nil {
		log.Fatal(err)
	}
	wTxn.Commit()

	txn := db.Txn(false)
	it, err := txn.Get("providers", "provider")
	if err != nil {
		log.Fatal(err)
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		fmt.Printf("entry: %#v\n", obj)
	}
}

//bug:1.注释掉第51行 2.allow missing应该是允许确实，但现在却是直接将前缀返回了