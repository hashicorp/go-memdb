package memdb

import "time"

// WatchSet is a collection of watch channels
type WatchSet map[<-chan struct{}]struct{}

// NewWatchSet constructs a new watch set
func NewWatchSet() WatchSet {
	return make(map[<-chan struct{}]struct{})
}

// Add appends a watchCh to the WatchSet if non-nil
func (w WatchSet) Add(watchCh <-chan struct{}) {
	if w == nil {
		return
	}
	if _, ok := w[watchCh]; !ok {
		w[watchCh] = struct{}{}
	}
}

// Watch is used to wait for either the watch set to trigger or a timeout
// Returns true on timeout
func (w WatchSet) Watch(timeoutCh <-chan time.Time) bool {
	if n := len(w); n <= 8 {
		return w.watchFew(timeoutCh)
	} else {
		return w.watchMany(timeoutCh)
	}
}

// watchFew is used if there are only a few watchers as a performance optimization
func (w WatchSet) watchFew(timeoutCh <-chan time.Time) bool {
	var ch1, ch2, ch3, ch4, ch5, ch6, ch7, ch8 <-chan struct{}
	idx := 0
	for watchCh := range w {
		switch idx {
		case 0:
			ch1 = watchCh
		case 1:
			ch2 = watchCh
		case 2:
			ch3 = watchCh
		case 3:
			ch4 = watchCh
		case 4:
			ch5 = watchCh
		case 5:
			ch6 = watchCh
		case 6:
			ch7 = watchCh
		case 7:
			ch8 = watchCh
		}
		idx++
	}
	select {
	case <-ch1:
		return false
	case <-ch2:
		return false
	case <-ch3:
		return false
	case <-ch4:
		return false
	case <-ch5:
		return false
	case <-ch6:
		return false
	case <-ch7:
		return false
	case <-ch8:
		return false
	case <-timeoutCh:
		return true
	}
}

// watchMany is used if there are many watchers
func (w WatchSet) watchMany(timeoutCh <-chan time.Time) bool {
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Start a goroutine for each watcher
	triggerCh := make(chan struct{}, 1)
	watcher := func(ch <-chan struct{}) {
		select {
		case <-ch:
			select {
			case triggerCh <- struct{}{}:
			default:
			}
		case <-doneCh:
			return
		}
	}
	for watchCh := range w {
		go watcher(watchCh)
	}

	// Wait for a channel to trigger or timeout
	select {
	case <-triggerCh:
		return false
	case <-timeoutCh:
		return true
	}
}
