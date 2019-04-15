package realtime

import (
	"sync"

	"github.com/iahmedov/eventagg"
	"github.com/iahmedov/eventagg/pkg/aggregator"
)

type (
	countAggregator struct {
		mtx    sync.Mutex
		counts map[string]int64
	}
)

const KeyEventType = "event_type"

func init() {
	aggregator.RegisterAggregator("realtime_count", NewCountAggregator)
}

func NewCountAggregator(aggregator.Config) (aggregator.Aggregator, error) {
	return &countAggregator{
		counts: map[string]int64{},
	}, nil
}

func (c *countAggregator) Add(ev *eventagg.Event) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.counts[ev.Type] = c.counts[ev.Type] + 1
	return nil
}

func (c *countAggregator) View(params ...aggregator.Param) (aggregator.Result, error) {
	for _, p := range params {
		if p.Key == KeyEventType {
			c.mtx.Lock()
			return func() (aggregator.Result, error) {
				defer c.mtx.Unlock()
				return c.counts[p.Value], nil
			}()
		}
	}
	return c.counts, nil
}

func (c *countAggregator) Close() error {
	return nil
}
