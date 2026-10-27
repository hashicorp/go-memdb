package memdb

import (
	"runtime"
	"testing"
)

func benchmarkTxnInsert(b *testing.B, batchSize int) {
	objects := make([]interface{}, b.N*batchSize)
	for i := 0; i < b.N*batchSize; i++ {
		objects[i] = testObjWithId(i) // Ensure valid objects
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := NewMemDB(testValidSchema())
		if err != nil {
			b.Fatalf("err: %v", err)
		}

		txn := db.Txn(true)
		start := i * batchSize
		for j := 0; j < batchSize; j++ {
			if err := txn.Insert("main", objects[start+j]); err != nil {
				b.Fatalf("insert failed: %v", err)
			}
		}
		txn.Commit()
	}
}

func benchmarkTxnBulkInsert(b *testing.B, batchSize int) {
	// Create a new memdb instance
	// Benchmark the insert operation
	objects := make([]interface{}, b.N*batchSize)
	for i := 0; i < b.N*batchSize; i++ {
		obj := testObjWithId(i)
		objects[i] = obj
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := make([]*TableData, 1)
		data[0] = &TableData{}
		data[0].Table = "main"
		data[0].Objects = objects[i*batchSize : (i+1)*batchSize]
		_, err := NewMemDBWithData(testValidSchema(), data, runtime.NumCPU())
		if err != nil {
			b.Fatalf("err: %v", err)
		}
	}
}

// Benchmark for insert operations
func Benchmark10TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 10)
}

func Benchmark10TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 10)
}

func Benchmark100TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 100)
}

func Benchmark100TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 100)
}

func Benchmark1000TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 1000)
}

func Benchmark1000TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 1000)
}

func Benchmark10000TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 10000)
}

func Benchmark10000TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 10000)
}

func Benchmark100000TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 100000)
}

func Benchmark100000TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 100000)
}

func Benchmark1000000TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 1000000)
}

func Benchmark1000000TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 1000000)
}

func Benchmark10000000TxnInsert(b *testing.B) {
	benchmarkTxnInsert(b, 10000000)
}

func Benchmark10000000TxnBulkInsert(b *testing.B) {
	benchmarkTxnBulkInsert(b, 10000000)
}
