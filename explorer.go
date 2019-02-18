package memdb

import "fmt"

// Explorer is used to view the data of database
// via an instance of Tnx
type Explorer interface {
	// ListAllTablesName lists all table names in database schema
	ListAllTablesName() ([]string, error)
	// TableDataView returns data of a table
	TableRecordsView(params TableRecordsViewParams) ([]interface{}, error)
	// CountRecords counts number records of given table
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

// Paginator is used to paginate data of a table
// Limit and Offset have same meaning to
// `SELECT column FROM table LIMIT limit OFFSET offset` in SQL
type Paginator interface {
	// GetLimit returns number of records per page
	GetLimit() uint64
	// GetCurrentPage returns current page of querying
	GetCurrentPage() uint64
	// GetOffset returns current offset of querying
	GetOffset() uint64
}

// TableDataViewParams is used to get query parameters
type TableRecordsViewParams interface {
	// GetTableName returns table that will be queried to
	GetTableName() string
	Paginator
	//TODO: FilterFunc FilterFunc
}

func (ge *explorer) TableRecordsView(params TableRecordsViewParams) ([]interface{}, error) {
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

// getTableIndexes returns all index names of given table
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

// NewExplorer inits new instance of Explorer
func NewExplorer(txn *Txn) Explorer {
	return &explorer{
		txn: txn,
	}
}
