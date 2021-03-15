package memdb

import (
	"reflect"
	"strconv"
	"testing"
)

type tableRecordsViewParamsTest struct {
	table       string
	limit       uint64
	currentPage uint64
}

func (p *tableRecordsViewParamsTest) GetTableName() string {
	return p.table
}

func (p *tableRecordsViewParamsTest) GetLimit() uint64 {
	return p.limit
}

func (p *tableRecordsViewParamsTest) GetOffset() uint64 {
	if p.currentPage == 0 {
		return 0
	}

	return p.limit * (p.currentPage - 1)
}

func (p *tableRecordsViewParamsTest) GetCurrentPage() uint64 {
	return p.currentPage
}

func newTestSchema(tables ...string) *DBSchema {
	var tbls = make(map[string]*TableSchema, 0)
	for _, tbl := range tables {
		tbls[tbl] = &TableSchema{
			Name: tbl,
			Indexes: map[string]*IndexSchema{
				"id": &IndexSchema{
					Name:    "id",
					Unique:  true,
					Indexer: &StringFieldIndex{Field: "IndexField"},
				},
			},
		}
	}

	return &DBSchema{Tables: tbls}
}

func txnWithMultipleTables(tables ...string) (*Txn, error) {
	schema := newTestSchema(tables...)
	// Create a new database
	db, err := NewMemDB(schema)
	if err != nil {
		return nil, err
	}

	return db.Txn(false), nil
}

func txnWithTableNoRecord(table string) (*Txn, error) {
	schema := newTestSchema(table)
	// Create a new database
	db, err := NewMemDB(schema)
	if err != nil {
		return nil, err
	}

	return db.Txn(false), nil
}

func txnWithTableAndRecords(table string) (*Txn, error) {
	type record struct{ IndexField string }

	schema := newTestSchema(table)
	// Create a new database
	db, err := NewMemDB(schema)
	if err != nil {
		return nil, err
	}
	// Create a write transaction
	txn := db.Txn(true)

	// Insert new 100 records
	var rcd *record
	var idxStr string
	for idx := int64(0); idx < 100; idx++ {
		idxStr = strconv.FormatInt(idx, 10)
		rcd = &record{"rcd-num-" + idxStr}
		if err := txn.Insert(table, rcd); err != nil {
			return nil, err
		}
	}
	// Commit the transaction
	txn.Commit()

	return db.Txn(false), nil
}

func Test_explorer_ListAllTablesName(t *testing.T) {
	// Schema has 1 table only
	txnSingleTbl, err := txnWithTableNoRecord("txnWithTableNoRecord")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	singleTblExplr := NewExplorer(txnSingleTbl)
	got, err := singleTblExplr.ListAllTablesName()
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	want := []string{"txnWithTableNoRecord"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", got, want)
	}

	// Schema has multiple tables
	want = []string{"txnWithMultipleTables1", "txnWithMultipleTables2"}
	txnMultipleTbl, err := txnWithMultipleTables(want...)
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	multipleTblExplr := NewExplorer(txnMultipleTbl)
	got, err = multipleTblExplr.ListAllTablesName()
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", got, want)
	}
}

func Test_explorer_TableRecordsView(t *testing.T) {
	// Table has no record
	txnTblNoRecord, err := txnWithTableNoRecord("txnWithTableNoRecord")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	tblNoRecordExplr := NewExplorer(txnTblNoRecord)
	params := &tableRecordsViewParamsTest{
		table: "txnWithTableNoRecord",
	}
	got, err := tblNoRecordExplr.TableRecordsView(params)
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if len(got) > 0 {
		t.Errorf("explorer.ListAllTablesName() = %v, want []interface{}{}", got)
	}

	// Table with existing record
	txnTblExistingRcd, err := txnWithTableAndRecords("txnWithTableAndRecords")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	tblExistingRcdExplr := NewExplorer(txnTblExistingRcd)

	// offset + limit < total records
	params = &tableRecordsViewParamsTest{
		table:       "txnWithTableAndRecords",
		limit:       15,
		currentPage: 1,
	}
	got, err = tblExistingRcdExplr.TableRecordsView(params)
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if len(got) != 15 {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", len(got), 15)
	}

	// offset + limit = total records
	params = &tableRecordsViewParamsTest{
		table:       "txnWithTableAndRecords",
		limit:       100,
		currentPage: 1,
	}
	got, err = tblExistingRcdExplr.TableRecordsView(params)
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if len(got) != 100 {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", len(got), 100)
	}

	// offset + limit > total records
	params = &tableRecordsViewParamsTest{
		table:       "txnWithTableAndRecords",
		limit:       10,
		currentPage: 11,
	}
	got, err = tblExistingRcdExplr.TableRecordsView(params)
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if len(got) != 0 {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", len(got), 0)
	}
}

func Test_explorer_CountRecords(t *testing.T) {
	// Table has no record
	txnNoRecord, err := txnWithTableNoRecord("txnWithTableNoRecord")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	noRecordExplr := NewExplorer(txnNoRecord)
	got, err := noRecordExplr.CountRecords("txnWithTableNoRecord")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if got != 0 {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", got, 0)
	}

	// Table has 100 records
	txnExistingRecords, err := txnWithTableAndRecords("txnWithTableAndRecords")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	existingRecordExplr := NewExplorer(txnExistingRecords)
	got, err = existingRecordExplr.CountRecords("txnWithTableAndRecords")
	if err != nil {
		t.Fatalf("should be no error %v", err)
	}
	if got != 100 {
		t.Errorf("explorer.ListAllTablesName() = %v, want %v", got, 100)
	}
}
