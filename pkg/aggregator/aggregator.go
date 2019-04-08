package aggregator

import (
	"fmt"
	"sync"

	"github.com/iahmedov/eventagg"

	"github.com/pkg/errors"
)

type (
	Collector interface {
		Add(ev *eventagg.Event) error
		Close() error
	}

	View interface {
		View(params ...Param) (Result, error)
	}

	Aggregator interface {
		Collector
		View
	}

	Config map[string]interface{}
	Param  struct {
		Key, Value string
	}
	Result interface{}

	factory func(Config) (Aggregator, error)
)

var (
	mtxAggregators sync.Mutex
	aggregators    = map[string]factory{}
)

func RegisterAggregator(name string, f factory) {
	mtxAggregators.Lock()
	defer mtxAggregators.Unlock()

	if _, ok := aggregators[name]; ok {
		panic(fmt.Sprintf("aggregator with %s already exist", name))
	}

	aggregators[name] = f
}

func New(name string, cfg Config) (Aggregator, error) {
	mtxAggregators.Lock()
	defer mtxAggregators.Unlock()

	f, ok := aggregators[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no aggregator with name %s", name))
	}

	return f(cfg)
}
