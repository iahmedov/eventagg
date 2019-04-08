package realtime

import (
	"fmt"
	"testing"

	"github.com/iahmedov/eventagg"
	"github.com/iahmedov/eventagg/pkg/aggregator"

	"github.com/stretchr/testify/require"
)

func TestCountAgg(t *testing.T) {
	agg, err := newCountAggregator(aggregator.Config{})
	require.NoError(t, err)

	for i := 0; i < 105; i++ {
		agg.Add(&eventagg.Event{
			Type: fmt.Sprintf("event_type_%d", int(i/10)),
		})
	}

	cases := []struct {
		eventType string
		count     int64
	}{
		{"event_type_0", 10},
		{"event_type_1", 10},
		{"event_type_10", 5},
		{"event_type_11", 0},
	}

	for _, c := range cases {
		t.Run(c.eventType, func(t *testing.T) {
			res, err := agg.View(aggregator.Param{
				Key:   KeyEventType,
				Value: c.eventType,
			})
			require.NoError(t, err)
			require.Equal(t, c.count, res)
		})
	}
}
