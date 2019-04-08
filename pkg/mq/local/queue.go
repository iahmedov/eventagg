package local

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/iahmedov/eventagg"

	"github.com/pkg/errors"
)

type eventHandler func(*eventagg.Event) error

type Queue struct {
	// when events are too fast
	// channel could be blocked too many times
	started int32
	ch      chan *eventagg.Event

	mtxSubscribers sync.Mutex
	subscribers    []eventHandler
}

const (
	// C/C++ style???????????????
	STARTED = 1
	STOPPED = 0
)

func New() *Queue {
	return &Queue{
		started:     STOPPED,
		ch:          make(chan *eventagg.Event, 100),
		subscribers: make([]eventHandler, 0),
	}
}

func (q *Queue) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&q.started, STOPPED, STARTED) {
		return errors.New("queue is already running")
	}

	defer func() {
		atomic.CompareAndSwapInt32(&q.started, STARTED, STOPPED)
		close(q.ch)
	}()

	for {
		select {
		case <-ctx.Done():
			break
		case ev := <-q.ch:
			for _, handler := range q.subscribers {
				handler(ev)
			}
		}
	}
	return nil
}

func (q *Queue) Insert(ev *eventagg.Event) error {
	if !q.isRunning() {
		return errors.New("queue is not running")
	}

	if ev == nil {
		return nil
	}
	q.ch <- ev
	return nil
}

func (q *Queue) Subscribe(f func(ev *eventagg.Event) error) error {
	if q.isRunning() {
		return errors.New("queue is already running")
	}

	q.subscribers = append(q.subscribers, f)
	return nil
}

func (q *Queue) isRunning() bool {
	return atomic.LoadInt32(&q.started) == STARTED
}
