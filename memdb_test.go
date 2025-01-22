// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"runtime"
	"testing"
	"time"
)

func TestMemDB_SingleWriter_MultiReader(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	tx1 := db.Txn(true)
	tx2 := db.Txn(false) // Should not block!
	tx3 := db.Txn(false) // Should not block!
	tx4 := db.Txn(false) // Should not block!

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		db.Txn(true)
	}()

	select {
	case <-doneCh:
		t.Fatalf("should not allow another writer")
	case <-time.After(10 * time.Millisecond):
	}

	tx1.Abort()
	tx2.Abort()
	tx3.Abort()
	tx4.Abort()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("should allow another writer")
	}
}

func TestMemDB_Snapshot(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add an object
	obj := testObj()
	txn := db.Txn(true)
	txn.Insert("main", obj)
	txn.Commit()

	// Clone the db
	db2 := db.Snapshot()

	// Remove the object
	txn = db.Txn(true)
	txn.Delete("main", obj)
	txn.Commit()

	// Object should exist in second snapshot but not first
	txn = db.Txn(false)
	out, err := txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Fatalf("should not exist %#v", out)
	}

	txn = db2.Txn(true)
	out, err = txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil {
		t.Fatalf("should exist")
	}
}

func TestNewMemDBWithData(t *testing.T) {
	// Create a new memdb instance
	// Benchmark the insert operation
	objects := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		obj := testObjWithId(i)
		objects[i] = obj
	}
	data := make([]*TableData, 1)
	data[0] = &TableData{}
	data[0].Table = "main"
	data[0].Objects = objects
	db, err := NewMemDBWithData(testValidSchema(), data, runtime.NumCPU())
	if err != nil {
		t.Fatalf("error initialized memdb with data")
	}
	txn := db.Txn(false)
	for _, obj := range objects {
		res, err := txn.First("main", "id", obj.(*TestObject).ID)
		if err != nil {
			t.Fatalf("error")
		}
		if res == nil {
			t.Fatalf("fatal")
		}
	}
}
