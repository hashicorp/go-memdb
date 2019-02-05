package memdb

type Explorer interface {
	ListAllTablesName() ([]string, error)
	TableDataView(params TableDataViewParams) ([]interface{}, error)
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

type TableDataViewParams struct {
	Table string
	Index string
	Limit uint64
	Offset uint64
	//TODO: FilterFunc FilterFunc
}

func (ge *explorer) TableDataView(params TableDataViewParams) ([]interface{}, error) {
	records := make([]interface{}, 0)
	ri, err := ge.txn.Get(params.Table, params.Index)
	if err != nil {
		return nil, err
	}

	limit, offset := params.Limit, params.Offset
	count := uint64(0)
	idx := uint64(0)
	for record := ri.Next(); record != nil; record = ri.Next() {
		idx ++

		if idx <= offset {
			continue
		}
		if count < limit {
			records = append(records, record)
			count ++
			continue
		}

		break
	}

	return records, nil
}

func NewExplorer(txn *Txn) Explorer {
	return &explorer{
		txn: txn,
	}
}