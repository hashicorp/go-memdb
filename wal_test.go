package memdb

import (
	"fmt"
	"testing"
)

func TestSimpleWAL(t *testing.T) {

	type TestStruct struct {
		Name string
	}

	ch := Change{
		Table:  "t",
		Before: TestStruct{Name: "Before"},
		After:  TestStruct{Name: "After"},
	}

	l := t.TempDir()
	w, _ := NewSimpleWAL(l)
	_ = w.WriteEntry(ch)

	c := w.Replay()

	for change := range c {
		fmt.Println(change.Table)
	}
}
