package memdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"

	"golang.org/x/exp/slices"
)

type WAL interface {
	WriteEntry(chg Change) error
	Replay() chan Change
}

type SimpleWAL struct {
	folder  string
	entries int

	mu sync.Mutex
}

// NewSimpleWAL creates a simple (and unsafe) wal where entries are
// written to a folder in ever increasing numbers which will sort
// lexicographically when attempting to replay them.
func NewSimpleWAL(location string) (*SimpleWAL, error) {
	e, err := os.ReadDir(location)
	if err != nil {
		return nil, err
	}

	return &SimpleWAL{
		folder:  location,
		entries: len(e),
	}, nil
}

// Replay implements WAL.
func (s *SimpleWAL) Replay() chan Change {
	ch := make(chan Change)

	go func() {
		entries, _ := os.ReadDir(s.folder)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}

		slices.Sort(names)

		for _, name := range names {
			var change Change

			data, _ := os.ReadFile(path.Join(s.folder, name))
			_ = json.Unmarshal(data, &change)

			ch <- change
		}

		close(ch)
	}()

	return ch
}

// WriteEntry implements WAL.
func (s *SimpleWAL) WriteEntry(chg Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fname := fmt.Sprintf("%012d.log", s.entries)
	s.entries += 1

	target := path.Join(s.folder, fname)
	data, err := json.Marshal(chg)
	if err != nil {
		return err
	}

	return os.WriteFile(target, data, 0644)
}

var _ WAL = (*SimpleWAL)(nil)
