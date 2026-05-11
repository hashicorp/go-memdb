package minidb

import (
	"github.com/hashicorp/go-memdb"
)

type Person struct {
	Email string
	Name  string
	Age   int
}

func main() {
	scheme := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"person": &memdb.TableSchema{
				Name: "person",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Email"},
					},
					"age": &memdb.IndexSchema{
						Name:    "age",
						Indexer: &memdb.IntFieldIndex{Field: "Age"},
					},
				},
			},
		},
	}
	db, err := memdb.NewMemDB(scheme)
	if err != nil {
		panic(err)
	}
	person := []*Person{
		&Person{Name: "fanyinghao", Age: 22, Email: "2824435408@qq.com"},
	}
	txn := db.Txn(true)
	for _, p := range person {
		if err := txn.Insert("person", p); err != nil {
			panic(err)
		}
	}
	txn.Commit()
	txn = db.Txn(false)
	defer txn.Abort()

	it, err := txn.Get("person", "id")
	if err != nil {
		panic(err)
	}
	for obj := it.Next(); obj != nil; obj = it.Next() {
		p := obj.(*Person)
		println(p.Name)
	}

}
