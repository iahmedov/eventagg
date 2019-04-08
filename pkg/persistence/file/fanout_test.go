package file

import (
	"sync"
	"testing"
	"time"

	"github.com/iahmedov/eventagg"

	"github.com/stretchr/testify/require"
)

func TestFanout(t *testing.T) {
	t.Parallel()

	fastCh := make(chan *eventagg.Event, 100)
	slowChannels := []chan *eventagg.Event{}

	var mtxCounters sync.Mutex
	workerCount := 10
	workCount := 1000
	counters := map[int]int{}
	for i := 0; i < workerCount; i++ {
		counters[i] = 0
	}
	for i := 0; i < workerCount; i++ {
		slowChannels = append(slowChannels, make(chan *eventagg.Event, 1))
		go func(idx int, ch chan *eventagg.Event) {
			for range ch {
				mtxCounters.Lock()
				counters[idx] = counters[idx] + 1
				mtxCounters.Unlock()
				<-time.After(time.Millisecond * 5)
			}
		}(i, slowChannels[i])
	}

	fanoutRoundRobin(fastCh, slowChannels...)
	for i := 0; i < workCount; i++ {
		fastCh <- &eventagg.Event{Time: int64(i)}
	}
	close(fastCh)
	<-time.After(time.Second * 1)

	for i := 0; i < workerCount; i++ {
		require.Equal(t, workCount/workerCount, counters[i])
	}
}
