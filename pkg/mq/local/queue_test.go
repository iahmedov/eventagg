package local

import (
	"context"
	"testing"

	"github.com/iahmedov/eventagg"

	"github.com/stretchr/testify/require"
)

func TestQueueFlow(t *testing.T) {
	q := New()

	// add subscribers
	callCount := 0
	callCounterFunc := func(ev *eventagg.Event) error {
		callCount++
		return nil
	}
	q.Subscribe(callCounterFunc)
	q.Subscribe(callCounterFunc)

	// when queue is not started
	require.Error(t, q.Insert(&eventagg.Event{})) // queue not started

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// make sure queue is going to be started
	ch := make(chan interface{}, 1)
	go func() {
		ch <- struct{}{}
		q.Start(ctx)
	}()
	<-ch

	// try to insert new subscriber
	require.Error(t, q.Subscribe(callCounterFunc))

	// insert 100 events
	for i := 0; i < 100; i++ {
		require.NoError(t, q.Insert(&eventagg.Event{}))
	}

	require.Equal(t, 200, callCount)
}

func TestInsertNil(t *testing.T) {
	q := New()

	callCount := 0
	q.Subscribe(func(ev *eventagg.Event) error {
		callCount++
		return nil
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	// make sure queue is going to be started
	ch := make(chan interface{}, 1)
	go func() {
		ch <- struct{}{}
		q.Start(ctx)
	}()
	<-ch

	require.NoError(t, q.Insert(nil))
	require.Equal(t, 0, callCount)
}
