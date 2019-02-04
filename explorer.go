package memdb

import "fmt"

type Explorer interface {
	ListAllTablesName() ([]string, error)
	TableDateView() ([]interface{}, error)
}

type explorer struct {
	txn *Txn
}

var gExplorer *explorer

func (ge *explorer) ListAllTablesName() ([]string, error) {
	tablesName := make([]string, 0)
	for tblName := range ge.txn.db.schema.Tables {
		tablesName = append(tablesName, tblName)
	}

	return tablesName, nil
}

func (ge *explorer) TableDateView() ([]interface{}, error) {
	result, err := ge.txn.First("person", "id", "joe@aol.com")
	if err != nil {
		return nil, err
	}

	return []interface{}{result}, nil
}

func InitGlobalExplorer(txn *Txn) {
	gExplorer = &explorer{
		txn: txn,
	}
}

func GetGlobalExplorer() (ge Explorer, err error) {
	if gExplorer == nil {
		err = fmt.Errorf("Global explorer was not inited by InitGlobalExplorer")
		return
	}

	return gExplorer, nil
}