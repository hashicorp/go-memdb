// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	iradix "github.com/hashicorp/go-immutable-radix"
)

// MemDB is an in-memory database providing Atomicity, Consistency, and
// Isolation from ACID. MemDB doesn't provide Durability since it is an
// in-memory database.
//
// MemDB provides a table abstraction to store objects (rows) with multiple
// indexes based on inserted values. The database makes use of immutable radix
// trees to provide transactions and MVCC.
//
// Objects inserted into MemDB are not copied. It is **extremely important**
// that objects are not modified in-place after they are inserted since they
// are stored directly in MemDB. It remains unsafe to modify inserted objects
// even after they've been deleted from MemDB since there may still be older
// snapshots of the DB being read from other goroutines.
type MemDB struct {
	schema  *DBSchema
	root    unsafe.Pointer // *iradix.Tree underneath
	primary bool

	wal WAL

	// There can only be a single writer at once
	writer sync.Mutex
}

// NewMemDB creates a new MemDB with the given schema and folder in which to
// store/load the WAL
func NewMemDB(schema *DBSchema, walLocation string) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}

	wal, err := NewSimpleWAL(walLocation)
	if err != nil {
		return nil, err
	}
	db.wal = wal

	// Reapply any entries recorded in the WAL
	db.applyWAL()

	return db, nil
}

// applyWAL iterates through the WAL for the database, and attempts
// to replay the logs against the current database, taking steps to
// ensure that it doesn't generate new log entries at the same time.
func (db *MemDB) applyWAL() {
	ch := db.wal.Replay()

	txn := db.NoLogWriteTxn()

	for change := range ch {
		fmt.Println("Writing", change.Table, change.After)
		err := txn.Insert(change.Table, change.After)
		fmt.Println(err)
	}

	txn.Commit()
}

// DBSchema returns schema in use for introspection.
//
// The method is intended for *read-only* debugging use cases,
// returned schema should *never be modified in-place*.
func (db *MemDB) DBSchema() *DBSchema {
	return db.schema
}

// getRoot is used to do an atomic load of the root pointer
func (db *MemDB) getRoot() *iradix.Tree {
	root := (*iradix.Tree)(atomic.LoadPointer(&db.root))
	return root
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
		wal:     db.wal,
	}

	return txn
}

// NoLogWriteTxn is used when re-applying the write-ahead-log to ensure
// that it can't log the re-insertion of previous entries.
func (db *MemDB) NoLogWriteTxn() *Txn {
	db.writer.Lock()

	txn := &Txn{
		db:      db,
		write:   true,
		rootTxn: db.getRoot().Txn(),
	}

	return txn
}

// Snapshot is used to capture a point-in-time snapshot  of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		schema:  db.schema,
		root:    unsafe.Pointer(db.getRoot()),
		primary: false,
	}
	return clone
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
