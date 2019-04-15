package file

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/iahmedov/eventagg"

	"github.com/pkg/errors"
)

type worker struct {
	open int32

	seek      int64
	filePath  string
	out       *os.File
	outWriter io.Writer
	idx       *os.File
	idxWriter io.Writer
}

// output format:
// - data.out - [json] per line
// - data.idx - [begin_pos,end_pos,ts] per line
// 		- begin_pos - begin position of event data
// 		- end_pos - end position of event data
// 		- ts - event timestamp
func DataFilePath(path string) string {
	return filepath.Join(path, "data.out")
}

func IndexFilePath(path string) string {
	return filepath.Join(path, "data.idx")
}

func newWorker(path string) (*worker, error) {
	w := &worker{
		open:      1,
		seek:      0,
		filePath:  path,
		out:       nil,
		outWriter: nil,
		idx:       nil,
		idxWriter: nil,
	}

	fileInfo, err := os.Stat(path)
	switch {
	case err != nil && os.IsNotExist(err):
		if err = os.Mkdir(path, 0777); err != nil {
			return nil, errors.Wrap(err, "failed to create folder")
		}
	case err != nil:
		return nil, errors.Wrap(err, "failed to read file info")
	default:
		if !fileInfo.IsDir() {
			return nil, errors.New("invalid type for data dir")
		}
	}

	fl, err := os.OpenFile(DataFilePath(path), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}
	fileInfo, err = fl.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read file size")
	}
	w.seek = fileInfo.Size()
	w.out = fl
	w.outWriter = fl

	idxFl, err := os.OpenFile(IndexFilePath(path), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open index file")
	}
	w.idx = idxFl
	w.idxWriter = idxFl

	return w, nil
}

func (w *worker) Add(ev *eventagg.Event) error {
	if !w.isOpen() {
		return errors.New("worker closed")
	}

	beginPos := w.seek
	endPos := int64(0)
	content, err := json.Marshal(ev)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}

	_, err = w.outWriter.Write(content)
	if err != nil {
		return errors.Wrap(err, "failed to write to file")
	}

	endPos = beginPos + int64(len(content))
	w.idxWriter.Write([]byte(fmt.Sprintf("%d,%d,%d\n", beginPos, endPos, ev.Time)))
	w.seek = endPos

	return nil
}

func (w *worker) Close() error {
	if !atomic.CompareAndSwapInt32(&w.open, 1, 0) {
		return errors.New("already closed")
	}

	w.idx.Sync()
	w.idxWriter = nil
	w.out.Sync()
	w.seek = 0
	w.filePath = ""
	w.outWriter = nil
	return w.out.Close()
}

func (w *worker) isOpen() bool {
	return atomic.LoadInt32(&w.open) == 1
}
