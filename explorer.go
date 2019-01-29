package memdb

type explorer struct {
	txn *Txn
}

var gExplorer *explorer

func InitExplorer(txn *Txn) {
	gExplorer = &explorer{
		txn: txn,
	}
}

func (ge *explorer) ListAllTablesName() []string {
	tablesName := make([]string, 0)
	for tblName := range ge.txn.db.schema.Tables {
		tablesName = append(tablesName, tblName)
	}

	return tablesName
}

func (ge *explorer) TableDateView() ([]interface{}, error) {
	result, err := ge.txn.First("person", "id", "joe@aol.com")
	if err != nil {
		return nil, err
	}

	return []interface{}{result}, nil
}

func GetGlobalConnector() *explorer {
	return gExplorer
}