package minidb

import (
	iradix "github.com/hashicorp/go-immutable-radix"
	"sync"
	"sync/atomic"
	"unsafe"
)

type MemDB struct {
	scheme  *DBSchema
	root    unsafe.Pointer
	primary bool
	writer  sync.Mutex
}

func NewMemDB(schema *DBSchema) (*MemDB, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	db := &MemDB{
		scheme:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *MemDB) getRoot() *iradix.Tree {
	root := (*iradix.Tree)(atomic.LoadPointer(&db.root))
	return root
}

func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
	}
	return txn
}

func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		scheme:  db.scheme,
		root:    unsafe.Pointer(db.getRoot()),
		primary: false,
	}
	return clone
}

func (db *MemDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.scheme.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
