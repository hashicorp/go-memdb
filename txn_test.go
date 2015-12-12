package memdb

import "testing"

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

func TestTxn_Find(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-first-object",
		Foo: "foo",
	}
	obj2 := &TestObject{
		ID:  "my-cool-object",
		Foo: "bar",
	}
	obj3 := &TestObject{
		ID:  "my-cool-second-object",
		Foo: "baz",
	}
	obj4 := &TestObject{
		ID:  "my-ordinary-fourth-object",
		Foo: "baz",
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
	err = txn.Insert("main", obj4)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.Find("main", "id", "my-cool")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// there should be two objects
	if len(raw) != 2 {
		t.Fatalf("bad: expected two objects, got: %#v", raw)
	}

	// the objects should not be equal
	if raw[0] == raw[1] {
		t.Fatalf("bad: expected non-equal objects, got: %#v", raw)
	}

	for _, result := range raw {
		if result != obj2 && result != obj3 {
			t.Fatalf("bad: object is not expected, got: %#v", raw)
		}
	}

	raw, err = txn.Find("main", "id", "my+")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(raw) != 0 {
		t.Fatalf("bad: expected zero objects, got: %#v", raw)
	}
}

func TestTxn_First_NonUnique_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "my-object",
		Foo: "abc",
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
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

func TestTxn_InsertDelete_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
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
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
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
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
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
}

func TestTxn_DeleteAll_Prefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-object",
		Foo: "abc",
	}
	obj2 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "xyz",
	}
	obj3 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "xyz",
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

func TestTxn_InsertGet_Prefix(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  "my-cool-thing",
		Foo: "foobarbaz",
	}
	obj2 := &TestObject{
		ID:  "my-other-cool-thing",
		Foo: "foozipzap",
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
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
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
