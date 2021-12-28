package minidb

type Changes []Change

type Change struct {
	Table      string
	Before     interface{}
	After      interface{}
	primaryKey []byte
}

func (m *Change) Created() bool {
	return m.Before == nil && m.After != nil
}

func (m *Change) Updated() bool {
	return m.Before != nil && m.After != nil
}

func (m *Change) Deleted() bool {
	return m.Before != nil && m.After == nil
}
