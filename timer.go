package scheduler

import (
	"sync"
	"time"
)

type Timer struct {
	mutex *sync.Mutex
	closed bool
	C chan time.Time
}

func NewTimer(dur time.Duration) *Timer {
	ch := make(chan time.Time, 1)
	t := &Timer{
		mutex: &sync.Mutex{},
		closed: false,
		C: ch,
	}
	go func() {
		time.Sleep(dur)
		now := time.Now()
		t.mutex.Lock()
		if !t.closed {
			t.C <- now
			close(t.C)
			t.closed = true
		}
	}()
	return t
}

func (t *Timer) Wait() (time.Time, bool) {
	xt, ok := <-t.C
	return xt, ok
}

func (t *Timer) Stop() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if !t.closed {
		close(t.C)
		t.closed = true
		return true
	}
	return false
}
