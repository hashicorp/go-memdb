package memdb

import (
	"reflect"
	"time"
)

// WatchSet is a collection of watch channels.
type WatchSet map[<-chan struct{}]struct{}

// NewWatchSet constructs a new watch set.
func NewWatchSet() WatchSet {
	return make(map[<-chan struct{}]struct{})
}

// Add appends a watchCh to the WatchSet if non-nil.
func (w WatchSet) Add(watchCh <-chan struct{}) {
	if w == nil {
		return
	}

	if _, ok := w[watchCh]; !ok {
		w[watchCh] = struct{}{}
	}
}

// AddWithLimit appends a watchCh to the WatchSet if non-nil, and if the given
// softLimit hasn't been exceeded. Otherwise, it will watch the given alternate
// channel. It's expected that the altCh will be the same on many calls to this
// function, so you will exceed the soft limit a little bit if you hit this, but
// not by much.
//
// This is useful if you want to track individual items up to some limit, after
// which you watch a higher-level channel (usually a channel from start start of
// an iterator higher up in the radix tree) that will watch a superset of items.
func (w WatchSet) AddWithLimit(softLimit int, watchCh <-chan struct{}, altCh <-chan struct{}) {
	// This is safe for a nil WatchSet so we don't need to check that here.
	if len(w) < softLimit {
		w.Add(watchCh)
	} else {
		w.Add(altCh)
	}
}

// Watch is used to wait for either the watch set to trigger or a timeout.
// Returns true on timeout.
func (w WatchSet) Watch(timeoutCh <-chan time.Time) bool {
	if w == nil {
		return false
	}

	selectCases := make([]reflect.SelectCase, 0, 1+len(w))

	// Add the timeout channel with index 0.
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(timeoutCh),
	})

	for watchCh := range w {
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(watchCh),
		})
	}

	chosenIndex, _, _ := reflect.Select(selectCases)

	return chosenIndex == 0
}
