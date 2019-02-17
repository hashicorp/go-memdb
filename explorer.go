package memdb

import "fmt"

type Explorer interface {
	ListAllTablesName() ([]string, error)
	TableDataView(params TableDataViewParams) ([]interface{}, error)
	CountRecords(table string) (uint64, error)
}

type explorer struct {
	txn *Txn
}

func (ge *explorer) ListAllTablesName() ([]string, error) {
	tablesName := make([]string, 0)
	for tblName := range ge.txn.db.schema.Tables {
		tablesName = append(tablesName, tblName)
	}

	return tablesName, nil
}

type Paginator interface {
	GetLimit() uint64
	GetCurrentPage() uint64
	GetOffset() uint64
}

type TableDataViewParams interface {
	GetTableName() string
	Paginator
	//TODO: FilterFunc FilterFunc
}

func (ge *explorer) TableDataView(params TableDataViewParams) ([]interface{}, error) {
	table := params.GetTableName()
	indexes, err := ge.getTableIndexes(table)
	if err != nil {
		return nil, err
	}

	ri, err := ge.txn.Get(table, indexes[0])
	if err != nil {
		return nil, err
	}

	records := make([]interface{}, 0)
	limit, offset := params.GetLimit(), params.GetOffset()
	count := uint64(0)
	idx := uint64(0)
	for record := ri.Next(); record != nil; record = ri.Next() {
		idx++

		if idx <= offset {
			continue
		}
		if count < limit {
			records = append(records, record)
			count++
			continue
		}

		break
	}

	return records, nil
}

func (ge *explorer) CountRecords(table string) (uint64, error) {
	var recordCnt uint64 = 0
	indexes, err := ge.getTableIndexes(table)
	if err != nil {
		return 0, err
	}

	ri, err := ge.txn.Get(table, indexes[0])
	if err != nil {
		return 0, err
	}

	for record := ri.Next(); record != nil; record = ri.Next() {
		recordCnt++
	}

	return recordCnt, nil
}

func (ge *explorer) getTableIndexes(table string) ([]string, error) {
	schema, ok := ge.txn.db.schema.Tables[table]
	if !ok {
		return nil, fmt.Errorf("Invalid table")
	}

	indexes := make([]string, 0)
	for idx, _ := range schema.Indexes {
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

func NewExplorer(txn *Txn) Explorer {
	return &explorer{
		txn: txn,
	}
}
