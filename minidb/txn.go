package minidb

import (
	"fmt"
	iradix "github.com/hashicorp/go-immutable-radix"
)

const (
	id = "id"
)

var (
	ErrNotFound = fmt.Errorf("not found")
)

type tableIndex struct {
	Table string
	Index string
}

type Txn struct {
	db      *MemDB
	write   bool
	rootTxn *iradix.Txn
	after   []func()

	changes Changes

	modified map[tableIndex]*iradix.Txn
}

func (txn *Txn) TrackChanges() {
	if txn.changes == nil {
		txn.changes = make(Changes, 0, 1)
	}
}
