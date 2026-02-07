// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
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
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Clone the db
	db2 := db.Snapshot()

	// Remove the object
	txn = db.Txn(true)
	if err := txn.Delete("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
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

func TestMemDB_SimpleCount(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "a",
		Foo: "xyz",
		Qux: []string{"xyz1"},
	}
	obj2 := &TestObject{
		ID:  "medium-length",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "super-long-unique-identifier",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
		Bar: 2,
	}

	// Pre-insert counts should be zero
	if count := db.Count("main", "id"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "foo"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "qux"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	err = txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Pre-commit counts should be zero
	if count := db.Count("main", "id"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "foo"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "qux"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn.Commit()

	// Post-commit counts should reflect the two inserted objects
	if count := db.Count("main", "id"); count != 2 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "foo"); count != 2 {
		t.Fatalf("bad count: %d", count)
	}

	// The "qux" index should have 3 entries because obj2 has two values in the "qux" field
	if count := db.Count("main", "qux"); count != 3 {
		t.Fatalf("bad count: %d", count)
	}

	txn = db.Txn(true)
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn.Commit()

	txn = db.Txn(false)
	defer txn.Abort()

	if count := db.Count("main", "id"); count != 3 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "foo"); count != 3 {
		t.Fatalf("bad count: %d", count)
	}

	if count := db.Count("main", "qux"); count != 5 {
		t.Fatalf("bad count: %d", count)
	}

	// non-index column not supported by Count and will return 0
	if count := db.Count("main", "bar"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}

	// non-existent column not supported by Count and will return 0
	if count := db.Count("main", "bazooka"); count != 0 {
		t.Fatalf("bad count: %d", count)
	}
}
