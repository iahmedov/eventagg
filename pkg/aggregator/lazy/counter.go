package cold

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/iahmedov/eventagg"
	"github.com/iahmedov/eventagg/pkg/aggregator"
	"github.com/iahmedov/eventagg/pkg/aggregator/realtime"

	"github.com/pkg/errors"
)

func init() {
	aggregator.RegisterAggregator("lazy_persistence_range_count", newPersistenceRangeCountAggregator)
}

const (
	KeyTimeRangeBefore = "before"
	KeyTimeRangeAfter  = "after"
	TimeFormat         = "2006-01-02T15:04:05"
)

type persistenceRangeCountAggregator struct {
	workerDirs []string
}

func newPersistenceRangeCountAggregator(cfg aggregator.Config) (aggregator.Aggregator, error) {
	dirIfc, ok := cfg["data_dir"]
	if !ok {
		return nil, errors.New("data directory not given for persistence based aggregator")
	}

	dir, ok := dirIfc.(string)
	if !ok {
		return nil, errors.New("data directory should be string type")
	}

	fl, err := os.Open(dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}
	folders, err := fl.Readdirnames(0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read child directories")
	}
	for i := range folders {
		folders[i] = filepath.Join(dir, folders[i])
	}

	return &persistenceRangeCountAggregator{
		workerDirs: folders,
	}, nil
}

func (p *persistenceRangeCountAggregator) Add(ev *eventagg.Event) error {
	return nil
}

func unixToTime(since int64) time.Time {
	return time.Unix(since, 0)
}

func (p *persistenceRangeCountAggregator) View(params ...aggregator.Param) (aggregator.Result, error) {

	// range, event type
	var begin, end *time.Time
	for i := range params {
		switch params[i].Key {
		case KeyTimeRangeAfter:
			b, err := time.Parse(TimeFormat, params[i].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse `after` time")
			}
			begin = &b
		case KeyTimeRangeBefore:
			e, err := time.Parse(TimeFormat, params[i].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse `before` time")
			}
			end = &e
		}
	}

	if begin == nil {
		b := unixToTime(0)
		begin = &b
	}
	if end == nil {
		now := time.Now().UTC()
		end = &now
	}

	results, errs := doParallel(func(dir string) (aggregator.Result, error) {
		reader, err := newTimeRangeReader(dir, *begin, *end)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create time range reader")
		}
		defer reader.Close()

		decoder := json.NewDecoder(reader)
		agg, err := realtime.NewCountAggregator(nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create count aggregator")
		}

		count := 0
		for decoder.More() {
			var ev eventagg.Event
			err = decoder.Decode(&ev)
			if err != nil {
				return nil, errors.Wrap(err, "failed to decode data")
			}
			agg.Add(&ev) // tolerate errors here
			count++
		}

		return agg.View()
	}, p.workerDirs...)
	_ = errs // skip errors for the sake of results
	return mergeCountResult(results...), nil
}

func mergeCountResult(results ...aggregator.Result) aggregator.Result {
	res := map[string]int64{}
	for i := range results {
		vals := results[i]
		for k, v := range vals.(map[string]int64) {
			if rv, ok := res[k]; ok {
				res[k] = rv + v
			} else {
				res[k] = v
			}
		}
	}
	return res
}

func (p *persistenceRangeCountAggregator) Close() error {
	return nil
}

func doParallel(f func(dir string) (aggregator.Result, error), workerDirs ...string) ([]aggregator.Result, []error) {
	errChan := make(chan error, len(workerDirs)+1)
	resChan := make(chan aggregator.Result, len(workerDirs)+1)

	workers := 0
	for i := range workerDirs {
		workers++
		go func(dir string) {
			res, err := f(dir)
			if err != nil {
				errChan <- err
			} else {
				resChan <- res
			}
		}(workerDirs[i])
	}

	results := []aggregator.Result{}
	errs := []error{}

	for {
		select {
		case e := <-errChan:
			workers--
			errs = append(errs, e)
		case r := <-resChan:
			workers--
			results = append(results, r)
		}
		if workers == 0 {
			break
		}
	}
	return results, errs
}
