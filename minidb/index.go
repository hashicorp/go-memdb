package minidb


type Indexer interface {
	FromArgs(args ...interface{}) ([]byte, error)
}

type SingleIndexer interface {
	FromObject(raw interface{}) (bool, []byte, error)
}

type MultiIndexer interface {
	FromObject(raw interface{}) (bool, [][]byte, error)
}

type PrefixIndexer interface {
	PrefixFromArgs(args ...interface{}) ([]byte, error)
}
