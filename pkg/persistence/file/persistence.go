package file

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iahmedov/eventagg"

	"github.com/pkg/errors"
)

type file struct {
	cfg     Config
	in      chan *eventagg.Event
	workers []*worker
}

func workerPath(dir string, idx int) string {
	return filepath.Join(dir, fmt.Sprintf("worker-%.6d", idx))
}

func New(cfg Config) (*file, error) {
	filePersistence := &file{
		cfg:     cfg,
		in:      make(chan *eventagg.Event, cfg.Count*10),
		workers: make([]*worker, cfg.Count),
	}

	fileInfo, err := os.Stat(cfg.DataDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read file info")
	}
	if !fileInfo.IsDir() {
		return nil, errors.Wrap(err, "invalid type for data dir")
	}

	for i := 0; i < cfg.Count; i++ {
		w, err := newWorker(workerPath(cfg.DataDir, i))
		if err != nil {
			// close previous open workers
			for j := i - 1; j >= 0; j-- {
				filePersistence.workers[j].Close()
			}
			return nil, errors.Wrap(err, "failed to create worker")
		}
		filePersistence.workers[i] = w
	}

	workerChannels := make([]chan *eventagg.Event, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		workerChannels[i] = make(chan *eventagg.Event, 1)
		go func(w *worker, ch chan *eventagg.Event) {
			for ev := range ch {
				w.Add(ev)
			}
			w.Close()
		}(filePersistence.workers[i], workerChannels[i])
	}
	fanoutRoundRobin(filePersistence.in, workerChannels...)
	return filePersistence, nil
}

func (f *file) Add(ev *eventagg.Event) error {
	// no need for ID
	f.in <- ev
	return nil
}

func (f *file) Close() error {
	close(f.in)
	return nil
}
