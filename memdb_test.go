package memdb

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type MemdbTestSuite struct {
	suite.Suite
}

func TestMemdb(t *testing.T) {
	suite.Run(t, new(MemdbTestSuite))
}

func (suite *MemdbTestSuite) TestSingleWriter_MultiReader() {
	db, err := NewMemDB(testValidSchema())
	suite.Require().NoError(err)

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
		suite.Fail("should not allow another writer")
	case <-time.After(10 * time.Millisecond):
	}

	tx1.Abort()
	tx2.Abort()
	tx3.Abort()
	tx4.Abort()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Millisecond):
		suite.Fail("should allow another writer")
	}
}

func (suite *MemdbTestSuite) TestSnapshot() {
	db, err := NewMemDB(testValidSchema())
	suite.Require().NoError(err)

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
	suite.Require().NoError(err)
	suite.Require().Nil(out)

	txn = db2.Txn(true)
	out, err = txn.First("main", "id", obj.ID)
	suite.Require().NoError(err)
	suite.Require().NotNil(out)
}

func (suite *MemdbTestSuite) TestCreateIndexes() {
	db, err := NewMemDB(testValidSchema())
	suite.Require().NoError(err)

	// Add an object
	obj := testObj()
	txn := db.Txn(true)
	txn.Insert("main", obj)
	txn.Commit()

	err = db.CreateIndexes("main", &IndexSchema{
		Name:         "FooIndex",
		AllowMissing: true,
		Unique:       false,
		Indexer: &StringFieldIndex{
			Field:     "Foo",
			Lowercase: false,
		},
	})
	suite.Require().NoError(err)

	idxIter, _, err := db.Txn(false).getIndexIterator("main", "foo")
	suite.Require().NoError(err)

	iter := &radixIterator{iter: idxIter}
	suite.NotNil(iter.Next())
}

func (suite *MemdbTestSuite) TestMemoryUsage_LateIndex() {
	db, err := NewMemDB(testValidSchema())
	suite.Require().NoError(err)

	suite.insertTestObjects(db)
	suite.createTestIndex(err, db)
	suite.insertTestObjects(db)

	runtime.GC()
	PrintMemUsage()

	idxIter, _, err := db.Txn(false).getIndexIterator("main", "Uint32MapIndex")
	suite.Require().NoError(err)

	iter := &radixIterator{iter: idxIter}
	count := 0
	for iter.Next() != nil {
		count += 1
	}
	fmt.Printf("count: %v\n", count)
}

func (suite *MemdbTestSuite) TestMemoryUsage_EarlyIndex() {
	db, err := NewMemDB(testValidSchema())
	suite.Require().NoError(err)

	suite.createTestIndex(err, db)
	suite.insertTestObjects(db)
	suite.insertTestObjects(db)

	runtime.GC()
	PrintMemUsage()

	idxIter, _, err := db.Txn(false).getIndexIterator("main", "Uint32MapIndex")
	suite.Require().NoError(err)

	iter := &radixIterator{iter: idxIter}
	count := 0
	for iter.Next() != nil {
		count += 1
	}
	fmt.Printf("count: %v\n", count)
}

func (suite *MemdbTestSuite) TestMemory_Combined() {
	type foo struct {
		ID   string
		Foo  uint32
		Foo1 uint32
		Foo2 uint32
		Foo3 int
		Foo4 int
	}

	db, err := NewMemDB(&DBSchema{
		Tables: map[string]*TableSchema{
			"main": {
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"qux": {
						Name:    "qux",
						Indexer: &StringSliceFieldIndex{Field: "Qux"},
					},
				},
			},
			"simple": {
				Name: "simple",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"Foo": {
						Name:   "Foo",
						Unique: false,
						Indexer: &UintFieldIndex{
							Field: "Foo",
						},
					},
				},
			},
		},
	})
	suite.Require().NoError(err)

	suite.insertTestObjects(db)
	suite.createTestIndex(err, db)

	PrintMemUsage()

	for i := 0; i < 1000000; i++ {
		txn := db.Txn(true)
		suite.Require().NoError(txn.Insert("simple", foo{
			ID:   fmt.Sprintf("id-%v", rand.Int()),
			Foo:  rand.Uint32(),
			Foo1: rand.Uint32(),
			Foo2: rand.Uint32(),
			Foo3: rand.Int(),
			Foo4: rand.Int(),
		}))
		txn.Commit()
	}

	PrintMemUsage()

	idxIter, _, err := db.Txn(false).getIndexIterator("simple", "id")
	suite.Require().NoError(err)

	iter := &radixIterator{iter: idxIter}
	count := 0
	for iter.Next() != nil {
		count += 1
	}
	fmt.Printf("count: %v\n", count)

	idxIter, _, err = db.Txn(false).getIndexIterator("main", "Uint32MapIndex")
	suite.Require().NoError(err)

	iter = &radixIterator{iter: idxIter}
	count = 0
	for iter.Next() != nil {
		count += 1
	}
	fmt.Printf("count: %v\n", count)
}

func (suite *MemdbTestSuite) TestMemory_Separate() {
	type foo struct {
		ID   string
		Foo  uint32
		Foo1 uint32
		Foo2 uint32
		Foo3 int
		Foo4 int
	}
	//
	//dbMain, err := NewMemDB(&DBSchema{
	//	Tables: map[string]*TableSchema{
	//		"main": {
	//			Name: "main",
	//			Indexes: map[string]*IndexSchema{
	//				"id": {
	//					Name:    "id",
	//					Unique:  true,
	//					Indexer: &StringFieldIndex{Field: "ID"},
	//				},
	//				"qux": {
	//					Name:    "qux",
	//					Indexer: &StringSliceFieldIndex{Field: "Qux"},
	//				},
	//			},
	//		},
	//	},
	//})
	//suite.Require().NoError(err)

	dbSimple, err := NewMemDB(&DBSchema{
		Tables: map[string]*TableSchema{
			"simple": {
				Name: "simple",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"Foo": {
						Name:   "Foo",
						Unique: false,
						Indexer: &UintFieldIndex{
							Field: "Foo",
						},
					},
				},
			},
		},
	})
	suite.Require().NoError(err)

	//suite.insertTestObjects(dbMain)
	//suite.createTestIndex(err, dbMain)

	//PrintMemUsage()

	for i := 0; i < 1000000; i++ {
		txn := dbSimple.Txn(true)
		suite.Require().NoError(txn.Insert("simple", foo{
			ID:   fmt.Sprintf("id-%v", rand.Int()),
			Foo:  rand.Uint32(),
			Foo1: rand.Uint32(),
			Foo2: rand.Uint32(),
			Foo3: rand.Int(),
			Foo4: rand.Int(),
		}))
		txn.Commit()
	}

	PrintMemUsage()

	idxIter, _, err := dbSimple.Txn(false).getIndexIterator("simple", "id")
	suite.Require().NoError(err)

	iter := &radixIterator{iter: idxIter}
	count := 0
	for iter.Next() != nil {
		count += 1
	}
	fmt.Printf("count: %v\n", count)
	//
	//idxIter, _, err = dbMain.Txn(false).getIndexIterator("main", "Uint32MapIndex")
	//suite.Require().NoError(err)
	//
	//iter = &radixIterator{iter: idxIter}
	//count = 0
	//for iter.Next() != nil {
	//	count += 1
	//}
	//fmt.Printf("count: %v\n", count)
}

func (suite *MemdbTestSuite) createTestIndex(err error, db *MemDB) {
	err = db.CreateIndexes("main", &IndexSchema{
		Name:         "FooIndex",
		AllowMissing: true,
		Unique:       false,
		Indexer: &StringFieldIndex{
			Field:     "Foo",
			Lowercase: false,
		},
	}, &IndexSchema{
		Name:         "Uint32MapIndex",
		AllowMissing: true,
		Unique:       false,
		Indexer: &UintSetFieldIndex{
			Field: "Uint32Map",
		},
	},
	)
	suite.Require().NoError(err)
}

func (suite *MemdbTestSuite) insertTestObjects(db *MemDB) {
	for i := 0; i < 100000; i++ {
		// Add an object
		obj := testObj()

		for i := 0; i < rand.Intn(500); i++ {
			obj.Uint32Map[uint32(i)] = struct{}{}
		}

		txn := db.Txn(true)
		txn.Insert("main", obj)
		txn.Commit()
	}
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Heap = %v MiB", bToMb(m.HeapAlloc))
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc)
	fmt.Printf("\tSys = %v MiB", m.Sys)
	fmt.Printf("\tStackInuse = %v MiB", m.StackInuse)
	fmt.Printf("\tStackSys = %v MiB", m.StackSys)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

type UintSetFieldIndex struct {
	Field string
}

var mapType = reflect.MapOf(reflect.TypeOf(uint64(0)), reflect.TypeOf(struct{}{})).Kind() //nolint:gochecknoglobals

func (index *UintSetFieldIndex) FromObject(obj interface{}) (bool, [][]byte, error) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v) // Dereference the pointer if any

	fv := v.FieldByName(index.Field)
	if !fv.IsValid() {
		return false, nil, fmt.Errorf("field '%s' for %#v is invalid", index.Field, obj)
	}

	if fv.Kind() != mapType {
		return false, nil, fmt.Errorf("field '%s' is not a map[uint64]struct{}{}", index.Field)
	}

	length := fv.Len()
	vals := make([][]byte, 0, length)
	for _, key := range fv.MapKeys() {
		//nolint:gomnd
		buf := make([]byte, 8)
		binary.BigEndian.PutUint32(buf, uint32(key.Uint()))
		vals = append(vals, buf)
	}
	if len(vals) == 0 {
		return false, nil, nil
	}
	return true, vals, nil
}

func (index *UintSetFieldIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide one argument")
	}
	key, ok := args[0].(uint32)
	if !ok {
		return nil, fmt.Errorf("argument must be a uint64: %#v", args[0])
	}
	//nolint:gomnd
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, key)
	return buf, nil
}
