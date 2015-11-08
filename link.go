package memdb

//Link is used for define relations between two objects
type Link struct {
	Table      string
	Index      string
	Arg1       interface{}
	Arg2       interface{}
	Attributes []string
}
