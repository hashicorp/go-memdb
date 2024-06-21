// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func testDB(t *testing.T) *MemDB {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}

func TestTxn_Read_AbortCommit(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(false) // Readonly

	txn.Abort()
	txn.Abort()
	txn.Commit()
	txn.Commit()
}

func TestTxn_Write_AbortCommit(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true) // Write

	txn.Abort()
	txn.Abort()
	txn.Commit()
	txn.Commit()

	txn = db.Txn(true) // Write

	txn.Commit()
	txn.Commit()
	txn.Abort()
	txn.Abort()
}

func TestTxn_Insert_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := testObj()
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}
}

func TestTxn_InsertUpdate_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}
}

func TestTxn_InsertUpdate_First_NonUnique(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}

	// Lookup of the old value should fail
	raw, err = txn.First("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_InsertUpdate_First_MultiIndex(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First("main", "qux", obj.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	raw, err = txn.First("main", "qux", obj.Qux[1])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}

	raw, err = txn.First("main", "qux", obj2.Qux[1])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}

	// Lookup of the old value should fail
	raw, err = txn.First("main", "qux", obj.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	raw, err = txn.First("main", "qux", obj.Qux[1])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_First_NonUnique_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The first object has a unique secondary value
	raw, err := txn.First("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}
}

func TestTxn_First_MultiIndex_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The first object has a unique secondary value
	raw, err := txn.First("main", "qux", obj.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}
}

func TestTxn_Last_NonUnique_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "xyz",
		Qux: []string{"abc1", "abc2"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "abc",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "abc",
		Qux: []string{"xyz1", "xyz2", "xyz3"},
	}

	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The last object has a unique secondary value
	raw, err := txn.Last("main", "foo", obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj3 should be last
	raw, err = txn.Last("main", "foo", obj3.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj3 {
		t.Fatalf("bad: %#v %#v", raw, obj3)
	}
}

func TestTxn_Last_MultiIndex_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2", "zyx1"},
	}

	err := txn.Insert("main", obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The last object has a unique secondary value
	raw, err := txn.Last("main", "qux", obj3.Qux[2])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj3 {
		t.Fatalf("bad: %#v %#v", raw, obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.Last("main", "qux", obj3.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj3 {
		t.Fatalf("bad: %#v %#v", raw, obj3)
	}
}
func TestTxn_InsertDelete_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err := txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj1 {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Commit and start a new transaction
	txn.Commit()
	txn = db.Txn(true)

	// Delete obj1
	err = txn.Delete("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete obj1 again and expect ErrNotFound
	err = txn.Delete("main", obj1)
	if err != ErrNotFound {
		t.Fatalf("expected err to be %v, got %v", ErrNotFound, err)
	}

	// Lookup of the primary obj1 should fail
	raw, err = txn.First("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Lookup of the primary obj1 should fail
	raw, err = txn.First("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != obj2 {
		t.Fatalf("bad: %#v %#v", raw, obj2)
	}
}

func TestTxn_InsertGet_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		// Attempt a row scan on the ID
		result, err := txn.Get("main", "id")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID
		result, err = txn.Get("main", "id", obj1.ID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.Get("main", "foo", obj1.Foo)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multi index
		result, err = txn.Get("main", "qux", obj1.Qux[0])
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		result, err = txn.Get("main", "qux", obj2.Qux[1])
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

func TestTxn_InsertGetReverse_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		// Attempt a row scan on the ID
		result, err := txn.GetReverse("main", "id")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID
		result, err = txn.GetReverse("main", "id", obj1.ID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.GetReverse("main", "foo", obj2.Foo)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multi index
		result, err = txn.GetReverse("main", "qux", obj2.Qux[0])
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		result, err = txn.GetReverse("main", "qux", obj1.Qux[1])
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

func TestTxn_DeleteAll_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc1"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Do a delete that doesn't hit any objects
	num, err := txn.DeleteAll("main", "id", "dogs")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 0 {
		t.Fatalf("bad: %d", num)
	}

	// Delete a specific ID
	num, err = txn.DeleteAll("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 1 {
		t.Fatalf("bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err := txn.First("main", "id", obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Delete an entire secondary range
	num, err = txn.DeleteAll("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 2 {
		t.Fatalf("Bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err = txn.First("main", "foo", obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Insert some more
	err = txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete an entire multiindex range
	num, err = txn.DeleteAll("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 2 {
		t.Fatalf("Bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err = txn.First("main", "qux", obj2.Qux[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_DeleteAll_Prefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc1"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete a prefix
	num, err := txn.DeleteAll("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 3 {
		t.Fatalf("bad: %d", num)
	}

	// Ensure we cannot lookup
	raw, err := txn.First("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_DeletePrefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
		Qux: []string{"abc1", "abc1"},
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
		Qux: []string{"xyz1", "xyz2"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Lookup by qux field index
	iterator, err := txn.Get("main", "qux", "abc1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var objects []TestObject
	for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
		object := obj.(*TestObject)
		objects = append(objects, *object)
	}
	if len(objects) != 1 {
		t.Fatalf("Expected exactly one object")
	}
	expectedID := "my-object"
	if objects[0].ID != expectedID {
		t.Fatalf("Unexpected id, expected %v, but got %v", expectedID, objects[0].ID)
	}

	// Delete a prefix
	deleted, err := txn.DeletePrefix("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	if !deleted {
		t.Fatalf("Expected DeletePrefix to return true")
	}

	// Ensure we cannot lookup by id field index
	raw, err := txn.First("main", "id_prefix", "my-")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if raw != nil {
		t.Fatalf("Unexpected value in tree: %#v", raw)
	}

	// Ensure we cannot lookup by qux or foo field indexes either anymore
	verifyNoResults(t, txn, "main", "qux", "abc1")
	verifyNoResults(t, txn, "main", "foo", "abc")
}
func verifyNoResults(t *testing.T, txn *Txn, table string, index string, value string) {
	iterator, err := txn.Get(table, index, value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if iterator != nil {
		next := iterator.Next()
		if next != nil {
			t.Fatalf("Unexpected values in tree, expected to be empty")
		}
	}
}

func TestTxn_InsertGet_Prefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "foobarbaz",
		Qux: []string{"foobarbaz", "fooqux"},
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "foozipzap",
		Qux: []string{"foozipzap"},
	}

	err := txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		// Attempt a row scan on the ID Prefix
		result, err := txn.Get("main", "id_prefix")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID prefix
		result, err = txn.Get("main", "id_prefix", "my-c")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.Get("main", "foo_prefix", "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index, tigher prefix
		result, err = txn.Get("main", "foo_prefix", "foob")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multiindex
		result, err = txn.Get("main", "qux_prefix", "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		// second index entry for obj1 (fooqux)
		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan multiindex, tigher prefix
		result, err = txn.Get("main", "qux_prefix", "foob")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); raw != obj1 {
			t.Fatalf("bad: %#v %#v", raw, obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

// CustomIndex is a simple custom indexer that doesn't add any suffixes to its
// object keys; this is compatible with the LongestPrefixMatch algorithm.
type CustomIndex struct{}

// FromObject takes the Foo field of a TestObject and prepends a null.
func (*CustomIndex) FromObject(obj interface{}) (bool, []byte, error) {
	t, ok := obj.(*TestObject)
	if !ok {
		return false, nil, fmt.Errorf("not a test object")
	}

	// Prepend a null so we can address an empty Foo field.
	out := "\x00" + t.Foo
	return true, []byte(out), nil
}

// FromArgs always returns an error.
func (*CustomIndex) FromArgs(args ...interface{}) ([]byte, error) {
	return nil, fmt.Errorf("only prefix lookups are supported")
}

// Prefix from args takes the argument as a string and prepends a null.
func (*CustomIndex) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("argument must be a string: %#v", args[0])
	}
	arg = "\x00" + arg
	return []byte(arg), nil
}

func TestTxn_InsertGet_LongestPrefix(t *testing.T) {
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"main": &TableSchema{
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:   "id",
						Unique: true,
						Indexer: &StringFieldIndex{
							Field: "ID",
						},
					},
					"foo": &IndexSchema{
						Name:    "foo",
						Unique:  true,
						Indexer: &CustomIndex{},
					},
					"nope": &IndexSchema{
						Name:    "nope",
						Indexer: &CustomIndex{},
					},
				},
			},
		},
	}

	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "object1",
		Foo: "foo",
	}
	obj2 := &TestObject{
		ID:  "object2",
		Foo: "foozipzap",
	}
	obj3 := &TestObject{
		ID:  "object3",
		Foo: "",
	}

	err = txn.Insert("main", obj1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert("main", obj3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *Txn) {
		raw, err := txn.LongestPrefix("main", "foo_prefix", "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foobar")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozip")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozipza")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj1 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozipzap")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj2 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "foozipzapzone")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj2 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "funky")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj3 {
			t.Fatalf("bad: %#v", raw)
		}

		raw, err = txn.LongestPrefix("main", "foo_prefix", "")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if raw != obj3 {
			t.Fatalf("bad: %#v", raw)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)

	// Try some disallowed index types.
	_, err = txn.LongestPrefix("main", "foo", "")
	if err == nil || !strings.Contains(err.Error(), "must use 'foo_prefix' on index") {
		t.Fatalf("bad: %v", err)
	}
	_, err = txn.LongestPrefix("main", "nope_prefix", "")
	if err == nil || !strings.Contains(err.Error(), "index 'nope_prefix' is not unique") {
		t.Fatalf("bad: %v", err)
	}
}

func TestTxn_Defer(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)
	res := ""

	// Defer a few functions
	txn.Defer(func() {
		res += "c"
	})
	txn.Defer(func() {
		res += "b"
	})
	txn.Defer(func() {
		res += "a"
	})

	// Commit the txn
	txn.Commit()

	// Check the result. All functions should have run, and should
	// have been executed in LIFO order.
	if res != "abc" {
		t.Fatalf("bad: %q", res)
	}
}

func TestTxn_Defer_Abort(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)
	res := ""

	// Defer a function
	txn.Defer(func() {
		res += "nope"
	})

	// Commit the txn
	txn.Abort()

	// Ensure deferred did not run
	if res != "" {
		t.Fatalf("bad: %q", res)
	}
}

func TestTxn_LowerBound(t *testing.T) {

	basicRows := []TestObject{
		{ID: "00001", Foo: "1", Qux: []string{"a"}},
		{ID: "00002", Foo: "2", Qux: []string{"a"}},
		{ID: "00004", Foo: "3", Qux: []string{"a"}},
		{ID: "00005", Foo: "4", Qux: []string{"a"}},
		{ID: "00010", Foo: "5", Qux: []string{"a"}},
		{ID: "10010", Foo: "6", Qux: []string{"a"}},
	}

	cases := []struct {
		Name   string
		Rows   []TestObject
		Search string
		Want   []TestObject
	}{
		{
			Name:   "all",
			Rows:   basicRows,
			Search: "0",
			Want:   basicRows,
		},
		{
			Name:   "subset existing bound",
			Rows:   basicRows,
			Search: "00005",
			Want: []TestObject{
				{ID: "00005", Foo: "4", Qux: []string{"a"}},
				{ID: "00010", Foo: "5", Qux: []string{"a"}},
				{ID: "10010", Foo: "6", Qux: []string{"a"}},
			},
		},
		{
			Name:   "subset non-existent bound",
			Rows:   basicRows,
			Search: "00006",
			Want: []TestObject{
				{ID: "00010", Foo: "5", Qux: []string{"a"}},
				{ID: "10010", Foo: "6", Qux: []string{"a"}},
			},
		},
		{
			Name:   "empty subset",
			Rows:   basicRows,
			Search: "99999",
			Want:   []TestObject{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			db := testDB(t)

			txn := db.Txn(true)
			for _, row := range tc.Rows {

				err := txn.Insert("main", row)
				if err != nil {
					t.Fatalf("err inserting: %s", err)
				}
			}
			txn.Commit()

			txn = db.Txn(false)
			defer txn.Abort()
			iterator, err := txn.LowerBound("main", "id", tc.Search)
			if err != nil {
				t.Fatalf("err lower bound: %s", err)
			}

			// Now range scan and built a result set
			result := []TestObject{}
			for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
				result = append(result, obj.(TestObject))
			}

			if !reflect.DeepEqual(result, tc.Want) {
				t.Fatalf(" got: %#v\nwant: %#v", result, tc.Want)
			}
		})
	}
}

func TestTxn_ReverseLowerBound(t *testing.T) {

	basicRows := []TestObject{
		{ID: "00101", Foo: "1", Qux: []string{"a"}},
		{ID: "00102", Foo: "2", Qux: []string{"a"}},
		{ID: "00104", Foo: "3", Qux: []string{"a"}},
		{ID: "00105", Foo: "4", Qux: []string{"a"}},
		{ID: "00110", Foo: "5", Qux: []string{"a"}},
		{ID: "10010", Foo: "6", Qux: []string{"a"}},
	}

	reverse := func(rows []TestObject) []TestObject {
		for i := 0; i < len(rows)/2; i++ {
			j := len(rows) - i - 1
			rows[i], rows[j] = rows[j], rows[i]
		}
		return rows
	}

	cases := []struct {
		Name   string
		Rows   []TestObject
		Search string
		Want   []TestObject
	}{
		{
			Name:   "all",
			Rows:   basicRows,
			Search: "99999",
			Want:   reverse(basicRows),
		},
		{
			Name:   "subset existing bound",
			Rows:   basicRows,
			Search: "00105",
			Want: []TestObject{
				{ID: "00105", Foo: "4", Qux: []string{"a"}},
				{ID: "00104", Foo: "3", Qux: []string{"a"}},
				{ID: "00102", Foo: "2", Qux: []string{"a"}},
				{ID: "00101", Foo: "1", Qux: []string{"a"}},
			},
		},
		{
			Name:   "subset non-existent bound",
			Rows:   basicRows,
			Search: "00103",
			Want: []TestObject{
				{ID: "00102", Foo: "2", Qux: []string{"a"}},
				{ID: "00101", Foo: "1", Qux: []string{"a"}},
			},
		},
		{
			Name:   "empty subset",
			Rows:   basicRows,
			Search: "0",
			Want:   []TestObject{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			db := testDB(t)

			txn := db.Txn(true)
			for _, row := range tc.Rows {

				err := txn.Insert("main", row)
				if err != nil {
					t.Fatalf("err inserting: %s", err)
				}
			}
			txn.Commit()

			txn = db.Txn(false)
			defer txn.Abort()
			iterator, err := txn.ReverseLowerBound("main", "id", tc.Search)
			if err != nil {
				t.Fatalf("err lower bound: %s", err)
			}

			// Now range scan and built a result set
			result := []TestObject{}
			for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
				result = append(result, obj.(TestObject))
			}

			if !reflect.DeepEqual(result, tc.Want) {
				t.Fatalf(" got: %#v\nwant: %#v", result, tc.Want)
			}
		})
	}
}

func TestTxn_Snapshot(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	err := txn.Insert("main", &TestObject{
		ID:  "one",
		Foo: "abc",
		Qux: []string{"abc1", "abc2"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	snapshot := txn.Snapshot()

	err = txn.Insert("main", &TestObject{
		ID:  "two",
		Foo: "def",
		Qux: []string{"def1", "def2"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn.Commit()

	raw, err := snapshot.First("main", "id", "one")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil || raw.(*TestObject).ID != "one" {
		t.Fatalf("TestObject one not found")
	}

	raw, err = snapshot.First("main", "id", "two")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("TestObject two found")
	}

	txn = db.Txn(false)
	snapshot = txn.Snapshot()

	raw, err = snapshot.First("main", "id", "one")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil || raw.(*TestObject).ID != "one" {
		t.Fatalf("TestObject one not found")
	}

	raw, err = snapshot.First("main", "id", "two")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil || raw.(*TestObject).ID != "two" {
		t.Fatalf("TestObject two not found")
	}
}

func TestStringFieldIndexerEmptyPointerFromArgs(t *testing.T) {
	t.Run("does not error with AllowMissing", func(t *testing.T) {
		schema := &DBSchema{
			Tables: map[string]*TableSchema{
				"main": &TableSchema{
					Name: "main",
					Indexes: map[string]*IndexSchema{
						"id": &IndexSchema{
							Name:   "id",
							Unique: true,
							Indexer: &StringFieldIndex{
								Field: "ID",
							},
						},
						"fu": &IndexSchema{
							Name: "fu",
							Indexer: &StringFieldIndex{
								Field: "Fu",
							},
							AllowMissing: true,
						},
					},
				},
			},
		}

		db, err := NewMemDB(schema)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		txn := db.Txn(true)

		s1 := "foo1"
		obj1 := &TestObject{
			ID: "object1",
			Fu: &s1,
		}

		obj2 := &TestObject{
			ID: "object2",
			Fu: nil,
		}

		err = txn.Insert("main", obj1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = txn.Insert("main", obj2)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	})

	t.Run("errors without AllowMissing", func(t *testing.T) {
		schema := &DBSchema{
			Tables: map[string]*TableSchema{
				"main": &TableSchema{
					Name: "main",
					Indexes: map[string]*IndexSchema{
						"id": &IndexSchema{
							Name:   "id",
							Unique: true,
							Indexer: &StringFieldIndex{
								Field: "ID",
							},
						},
						"fu": &IndexSchema{
							Name: "fu",
							Indexer: &StringFieldIndex{
								Field: "Fu",
							},
							AllowMissing: false,
						},
					},
				},
			},
		}

		db, err := NewMemDB(schema)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		txn := db.Txn(true)

		s1 := "foo1"
		obj1 := &TestObject{
			ID: "object1",
			Fu: &s1,
		}

		obj2 := &TestObject{
			ID: "object2",
			Fu: nil,
		}

		err = txn.Insert("main", obj1)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		err = txn.Insert("main", obj2)
		if err == nil {
			t.Fatalf("expected err not to be nil")
		}
	})
}

func TestTxn_Changes(t *testing.T) {

	// Create a schmea that exercises all mutation code paths (i.e. has a prefix
	// index as well as primary and multple tables).
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"one": &TableSchema{
				Name: "one",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:   "id",
						Unique: true,
						Indexer: &StringFieldIndex{
							Field: "ID",
						},
					},
					"foo": &IndexSchema{
						Name: "foo",
						Indexer: &StringFieldIndex{
							Field: "Foo",
						},
						AllowMissing: true,
					},
				},
			},
			"two": &TableSchema{
				Name: "two",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:   "id",
						Unique: true,
						Indexer: &StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},
		},
	}

	basicRows := []TestObject{
		{ID: "00001", Foo: "aaaaaaa"},
		{ID: "00002", Foo: "aaaaaab"},
		{ID: "00004", Foo: "aaabbbb"},
		{ID: "00005", Foo: "aabbbcc"},
		{ID: "00010", Foo: "bbccccc"},
		{ID: "10010", Foo: "ccccddd"},
	}

	mutatedRows := []TestObject{
		{ID: "00001", Foo: "changed"},
		{ID: "00002", Foo: "changed"},
		{ID: "00004", Foo: "changed"},
		{ID: "00005", Foo: "changed"},
		{ID: "00010", Foo: "changed"},
		{ID: "10010", Foo: "changed"},
	}

	mutated2Rows := []TestObject{
		{ID: "00001", Foo: "changed again"},
	}

	cases := []struct {
		Name            string
		TrackingEnabled bool
		OneRows         []TestObject
		TwoRows         []TestObject
		Mutate          func(t *testing.T, tx *Txn)
		Abort           bool
		WantChanges     Changes
	}{
		{
			Name:            "tracking disabled",
			TrackingEnabled: false,
			OneRows:         nil,
			TwoRows:         nil,
			Mutate: func(t *testing.T, tx *Txn) {
				err := tx.Insert("one", basicRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				err = tx.Insert("one", basicRows[1])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				err = tx.Insert("two", basicRows[2])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: nil,
		},
		{
			Name:            "tracking enabled, basic inserts",
			TrackingEnabled: true,
			OneRows:         nil,
			TwoRows:         nil,
			Mutate: func(t *testing.T, tx *Txn) {
				err := tx.Insert("one", basicRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				err = tx.Insert("one", basicRows[1])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				err = tx.Insert("two", basicRows[2])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				Change{
					Table:  "one",
					Before: nil,
					After:  basicRows[0],
				},
				Change{
					Table:  "one",
					Before: nil,
					After:  basicRows[1],
				},
				Change{
					Table:  "two",
					Before: nil,
					After:  basicRows[2],
				},
			},
		},
		{
			Name:            "tracking enabled, tx aborts",
			TrackingEnabled: true,
			OneRows:         nil,
			TwoRows:         nil,
			Mutate: func(t *testing.T, tx *Txn) {
				err := tx.Insert("one", basicRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				err = tx.Insert("one", basicRows[1])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				err = tx.Insert("two", basicRows[2])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			Abort:       true,
			WantChanges: nil,
		},
		{
			Name:            "mixed insert, update, delete",
			TrackingEnabled: true,
			OneRows:         []TestObject{basicRows[0]},
			TwoRows:         []TestObject{basicRows[2]},
			Mutate: func(t *testing.T, tx *Txn) {
				// Insert a new row
				err := tx.Insert("one", basicRows[1])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				// Update an existing row
				err = tx.Insert("one", mutatedRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				// Delete an existing row
				err = tx.Delete("two", basicRows[2])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				Change{
					Table:  "one",
					Before: nil,
					After:  basicRows[1],
				},
				Change{
					Table:  "one",
					Before: basicRows[0],
					After:  mutatedRows[0],
				},
				Change{
					Table:  "two",
					Before: basicRows[2],
					After:  nil,
				},
			},
		},
		{
			Name:            "mutate rows in same txn",
			TrackingEnabled: true,
			OneRows:         []TestObject{},
			TwoRows:         []TestObject{},
			Mutate: func(t *testing.T, tx *Txn) {
				// Insert a new row
				err := tx.Insert("one", basicRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				// Mutate same row again
				err = tx.Insert("one", mutatedRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				// Mutate same row again
				err = tx.Insert("one", mutated2Rows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				// Changes should only include a single object mutation going from
				// nothing before to the final value.
				Change{
					Table:  "one",
					Before: nil,
					After:  mutated2Rows[0],
				},
			},
		},
		{
			Name:            "mutate and delete in same txn",
			TrackingEnabled: true,
			OneRows:         []TestObject{basicRows[0]},
			TwoRows:         []TestObject{},
			Mutate: func(t *testing.T, tx *Txn) {
				// Update a new row
				err := tx.Insert("one", mutatedRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				// Mutate same row again
				err = tx.Delete("one", mutatedRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				// Changes should only include a single delete
				Change{
					Table:  "one",
					Before: basicRows[0],
					After:  nil,
				},
			},
		},
		{
			Name:            "delete prefix",
			TrackingEnabled: true,
			OneRows:         basicRows,
			TwoRows:         []TestObject{},
			Mutate: func(t *testing.T, tx *Txn) {
				// Delete Prefix
				_, err := tx.DeletePrefix("one", "foo_prefix", "aaa")
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				// First three rows should be removed
				Change{
					Table:  "one",
					Before: basicRows[0],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: basicRows[1],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: basicRows[2],
					After:  nil,
				},
			},
		},
		{
			Name:            "delete all",
			TrackingEnabled: true,
			OneRows:         mutatedRows,
			TwoRows:         mutatedRows,
			Mutate: func(t *testing.T, tx *Txn) {
				// Delete All rows
				_, err := tx.DeleteAll("one", "foo", "changed")
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				// All rows should be removed
				Change{
					Table:  "one",
					Before: mutatedRows[0],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: mutatedRows[1],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: mutatedRows[2],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: mutatedRows[3],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: mutatedRows[4],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: mutatedRows[5],
					After:  nil,
				},
			},
		},
		{
			Name:            "delete all partial",
			TrackingEnabled: true,
			OneRows: []TestObject{
				// Half the rows have unique Foo values half have "changed"
				basicRows[0], basicRows[1],
				mutatedRows[2], mutatedRows[3],
			},
			TwoRows: mutatedRows,
			Mutate: func(t *testing.T, tx *Txn) {
				// Delete All rows
				_, err := tx.DeleteAll("one", "foo", "changed")
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				// Only the matching rows should be removed
				Change{
					Table:  "one",
					Before: mutatedRows[2],
					After:  nil,
				},
				Change{
					Table:  "one",
					Before: mutatedRows[3],
					After:  nil,
				},
			},
		},
		{
			Name:            "insert and then delete same item in one txn",
			TrackingEnabled: true,
			Mutate: func(t *testing.T, tx *Txn) {
				// Insert a new row
				err := tx.Insert("one", basicRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
				// Delete the same row again
				err = tx.Delete("one", basicRows[0])
				if err != nil {
					t.Fatalf("Err: %s", err)
				}
			},
			WantChanges: Changes{
				// Whole transaction is a big no-op. Initial implementation missed this
				// edge case and emitted a mutation where both before and after were nil
				// which violates expectations in caller.
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			db, err := NewMemDB(schema)
			if err != nil {
				t.Fatalf("Failed to create DB: %s", err)
			}

			// Insert initial rows
			tx := db.Txn(true)
			for i, r := range tc.OneRows {
				err = tx.Insert("one", r)
				if err != nil {
					t.Fatalf("Failed to insert OneRows[%d]: %s", i, err)
				}
			}
			for i, r := range tc.TwoRows {
				err = tx.Insert("two", r)
				if err != nil {
					t.Fatalf("Failed to insert TwoRows[%d]: %s", i, err)
				}
			}
			tx.Commit()

			// Do test mutation
			tx2 := db.Txn(true)

			if tc.TrackingEnabled {
				tx2.TrackChanges()
			}

			tc.Mutate(t, tx2)

			if tc.Abort {
				tx2.Abort()
				gotAfterCommit := tx2.Changes()
				if !reflect.DeepEqual(gotAfterCommit, tc.WantChanges) {
					t.Fatalf("\n gotAfterCommit: %#v\n           want: %#v",
						gotAfterCommit, tc.WantChanges)
				}
				return
			}

			gotBeforeCommit := tx2.Changes()
			tx2.Commit()
			gotAfterCommit := tx2.Changes()

			// nil out the keys in Wanted since those are an implementation detail
			for i := range gotBeforeCommit {
				gotBeforeCommit[i].primaryKey = nil
			}
			for i := range gotAfterCommit {
				gotAfterCommit[i].primaryKey = nil
			}

			if !reflect.DeepEqual(gotBeforeCommit, tc.WantChanges) {
				t.Fatalf("\n gotBeforeCommit: %#v\n            want: %#v",
					gotBeforeCommit, tc.WantChanges)
			}
			if !reflect.DeepEqual(gotAfterCommit, tc.WantChanges) {
				t.Fatalf("\n gotAfterCommit: %#v\n           want: %#v",
					gotAfterCommit, tc.WantChanges)
			}
		})
	}
}

func TestTxn_GetIterAndDelete(t *testing.T) {
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"main": {
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"foo": {
						Name:    "foo",
						Indexer: &StringFieldIndex{Field: "Foo"},
					},
				},
			},
		},
	}
	db, err := NewMemDB(schema)
	assertNilError(t, err)

	key := "aaaa"
	txn := db.Txn(true)
	assertNilError(t, txn.Insert("main", &TestObject{ID: "1", Foo: key}))
	assertNilError(t, txn.Insert("main", &TestObject{ID: "123", Foo: key}))
	assertNilError(t, txn.Insert("main", &TestObject{ID: "2", Foo: key}))
	txn.Commit()

	txn = db.Txn(true)
	// Delete something
	assertNilError(t, txn.Delete("main", &TestObject{ID: "123", Foo: key}))

	iter, err := txn.Get("main", "foo", key)
	assertNilError(t, err)

	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		assertNilError(t, txn.Delete("main", obj))
	}

	txn.Commit()
}

func assertNilError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
