// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"testing"
)

func TestMemDB_Isolation(t *testing.T) {

	id1 := "object-one"
	id2 := "object-two"
	id3 := "object-three"

	setup := func(t *testing.T) *MemDB {
		t.Helper()

		db, err := NewMemDB(testValidSchema())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		// Add two objects (with a gap between their IDs)
		obj1a := testObj()
		obj1a.ID = id1
		txn := db.Txn(true)
		txn.Insert("main", obj1a)

		obj3 := testObj()
		obj3.ID = id3
		txn.Insert("main", obj3)
		txn.Commit()
		return db
	}

	t.Run("snapshot dirty read", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		txn1.Insert("main", obj1b)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		txn1.Insert("main", obj2)

		txn2 := db2.Txn(false)
		out, err := txn2.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.(*TestObject).Baz == "nope" {
			t.Fatalf("read from snapshot should not observe uncommitted update (dirty read)")
		}

		out, err = txn2.First("main", "id", id2)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != nil {
			t.Fatalf("read from snapshot should not observe uncommitted insert (dirty read)")
		}

		// New snapshot should not observe uncommitted writes
		db3 := db.Snapshot()
		txn3 := db3.Txn(false)
		out, err = txn3.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.(*TestObject).Baz == "nope" {
			t.Fatalf("read from new snapshot should not observe uncommitted writes")
		}
	})

	t.Run("transaction dirty read", func(t *testing.T) {
		db := setup(t)

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		txn1.Insert("main", obj1b)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		txn1.Insert("main", obj2)

		txn2 := db.Txn(false)
		out, err := txn2.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.(*TestObject).Baz == "nope" {
			t.Fatalf("read from transaction should not observe uncommitted update (dirty read)")
		}

		out, err = txn2.First("main", "id", id2)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != nil {
			t.Fatalf("read from transaction should not observe uncommitted insert (dirty read)")
		}
	})

	t.Run("snapshot non-repeatable read", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		txn1.Insert("main", obj1b)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		txn1.Insert("main", obj2)

		// Commit
		txn1.Commit()

		txn2 := db2.Txn(false)
		out, err := txn2.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.(*TestObject).Baz == "nope" {
			t.Fatalf("read from snapshot should not observe committed write from another transaction (non-repeatable read)")
		}

		out, err = txn2.First("main", "id", id2)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != nil {
			t.Fatalf("read from snapshot should not observe committed write from another transaction (non-repeatable read)")
		}

	})

	t.Run("transaction non-repeatable read", func(t *testing.T) {
		db := setup(t)

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		txn1.Insert("main", obj1b)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		txn1.Insert("main", obj2)

		txn2 := db.Txn(false)

		// Commit
		txn1.Commit()

		out, err := txn2.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.(*TestObject).Baz == "nope" {
			t.Fatalf("read from transaction should not observe committed write from another transaction (non-repeatable read)")
		}

		out, err = txn2.First("main", "id", id2)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out != nil {
			t.Fatalf("read from transaction should not observe committed write from another transaction (non-repeatable read)")
		}

	})

	t.Run("snapshot phantom read", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		txn2 := db2.Txn(false)
		iter, err := txn2.Get("main", "id_prefix", "object")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		out := iter.Next()
		if out == nil || out.(*TestObject).ID != id1 {
			t.Fatal("missing expected object 'object-one'")
		}

		// Insert an object and commit
		txn1 := db.Txn(true)
		obj2 := testObj()
		obj2.ID = id2
		txn1.Insert("main", obj2)
		txn1.Commit()

		out = iter.Next()
		if out == nil {
			t.Fatal("expected 2 objects")
		}
		if out.(*TestObject).ID == id2 {
			t.Fatalf("read from snapshot should not observe new objects in set (phantom read)")
		}

		out = iter.Next()
		if out != nil {
			t.Fatal("expected only 2 objects: read from snapshot should not observe new objects in set (phantom read)")
		}

		// Remove an object using an outdated pointer
		txn1 = db.Txn(true)
		obj1, err := txn1.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		txn1.Delete("main", obj1)
		txn1.Commit()

		iter, err = txn2.Get("main", "id_prefix", "object")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		out = iter.Next()
		if out == nil || out.(*TestObject).ID != id1 {
			t.Fatal("missing expected object 'object-one': read from snapshot should not observe deletes (phantom read)")
		}
		out = iter.Next()
		if out == nil || out.(*TestObject).ID != id3 {
			t.Fatal("missing expected object 'object-three': read from snapshot should not observe deletes (phantom read)")
		}

	})

	t.Run("transaction phantom read", func(t *testing.T) {
		db := setup(t)

		txn2 := db.Txn(false)
		iter, err := txn2.Get("main", "id_prefix", "object")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		out := iter.Next()
		if out == nil || out.(*TestObject).ID != id1 {
			t.Fatal("missing expected object 'object-one'")
		}

		// Insert an object and commit
		txn1 := db.Txn(true)
		obj2 := testObj()
		obj2.ID = id2
		txn1.Insert("main", obj2)
		txn1.Commit()

		out = iter.Next()
		if out == nil {
			t.Fatal("expected 2 objects")
		}
		if out.(*TestObject).ID == id2 {
			t.Fatalf("read from transaction should not observe new objects in set (phantom read)")
		}

		out = iter.Next()
		if out != nil {
			t.Fatal("expected only 2 objects: read from transaction should not observe new objects in set (phantom read)")
		}

		// Remove an object using an outdated pointer
		txn1 = db.Txn(true)
		obj1, err := txn1.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		txn1.Delete("main", obj1)
		txn1.Commit()

		iter, err = txn2.Get("main", "id_prefix", "object")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		out = iter.Next()
		if out == nil || out.(*TestObject).ID != id1 {
			t.Fatal("missing expected object 'object-one': read from transaction should not observe deletes (phantom read)")
		}
		out = iter.Next()
		if out == nil || out.(*TestObject).ID != id3 {
			t.Fatal("missing expected object 'object-three': read from transaction should not observe deletes (phantom read)")
		}

	})

	t.Run("snapshot commits are unobservable", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		txn2 := db2.Txn(true)
		obj1 := testObj()
		obj1.ID = id1
		obj1.Baz = "also"
		txn2.Insert("main", obj1)
		txn2.Commit()

		txn1 := db.Txn(false)
		out, err := txn1.First("main", "id", id1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.(*TestObject).Baz == "also" {
			t.Fatalf("commit from snapshot should never be observed")
		}
	})
}
