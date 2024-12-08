package memdb

import (
	"strconv"
	"testing"
)

// Benchmark for insert operations
func BenchmarkTxnInsert(b *testing.B) {
	// Create a new memdb instance
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	// Benchmark the insert operation
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj := testObjWithId(i)
		// Start a write transaction
		txn := db.Txn(true)
		// Insert an object
		err := txn.Insert("main", obj)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
		// Commit the transaction
		txn.Commit()
	}
}

// Benchmark for read operations
func BenchmarkTxnRead(b *testing.B) {
	// Prepopulate the database with data
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}
	txn := db.Txn(true)
	for i := 0; i < 10000; i++ {
		obj := testObjWithId(i)
		err := txn.Insert("main", obj)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
	}
	txn.Commit()

	// Benchmark the read operation
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.Txn(false) // Read-only transaction
		_, err := txn.Get("main", "id", strconv.Itoa(i%10000))
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
}
