package cold

import (
	"bufio"
	"io"
	"math"
	"os"
	"time"

	pfile "github.com/iahmedov/eventagg/pkg/persistence/file"

	"github.com/pkg/errors"
)

// timeRangeReader implements io.ReadCloser
type timeRangeReader struct {
	data *os.File

	begin, end int64
}

func newTimeRangeReader(dataDir string, start, end time.Time) (*timeRangeReader, error) {
	dataPath := pfile.DataFilePath(dataDir)
	indexPath := pfile.IndexFilePath(dataDir)

	// data file
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open data file")
	}

	// index file
	indexFile, err := os.Open(indexPath)
	if err != nil {
		dataFile.Close()
		return nil, errors.Wrap(err, "failed to open index file")
	}
	defer indexFile.Close()

	indexFileInfo, err := indexFile.Stat()
	if err != nil {
		dataFile.Close()
		return nil, errors.Wrap(err, "failed to read index file info")
	}

	// find range
	dataFileBegin, dataFileEnd, err := findTimeRange(indexFile,
		start.Unix(), end.Unix(), indexFileInfo.Size())
	if err != nil {
		dataFile.Close()
		return nil, errors.Wrap(err, "failed to find timed range")
	}

	if dataFileBegin < dataFileEnd {
		dataFile.Seek(dataFileBegin, os.SEEK_SET)
	}
	return &timeRangeReader{
		data:  dataFile,
		begin: dataFileBegin,
		end:   dataFileEnd,
	}, nil
}

func abs(a int64) int64 {
	// not handling overflow
	if a < 0 {
		return -1 * a
	}
	return a
}

func findTimeRange(reader io.ReadSeeker, bTime, eTime int64, endPos int64) (beginIdx, endIdx int64, err error) {
	var smallestTriplet, biggestTriplet *triplet
	var smallestDiff int64 = math.MaxInt64

	// find smallest value >= bTime
	var begin, end int64 = 0, endPos
	for begin < end {
		mid := begin + (end-begin)/2
		line, err := getLine(reader, mid)
		if err != nil {
			return 0, 0, errors.Wrap(err, "failed to get line")
		}

		t, err := parseTriplet(line)
		switch {
		case err != nil && err == errNotATriplet:
			// happens when reading non flushed file, skip
			end = end - 1
			continue
		case err != nil && err == errInvalidTriplet:
			return 0, 0, errors.Wrap(err, "failed to read line")
		}

		if t.ts < bTime {
			begin = mid + 1
			continue
		}

		if abs(t.ts-bTime) <= smallestDiff {
			smallestDiff = abs(t.ts - bTime)
			if smallestTriplet == nil || t.isSmaller(smallestTriplet) {
				smallestTriplet = t
			}
			end = mid - 1
		} else {
			begin = mid + 1
		}
	}
	if smallestTriplet == nil {
		// smallest item in file is bigger than given interval range
		return 0, 0, nil
	}

	// find biggest value <= eTime
	end = endPos
	smallestDiff = math.MaxInt64
	for begin < end {
		mid := begin + (end-begin)/2
		line, err := getLine(reader, mid)
		if err != nil {
			return 0, 0, errors.Wrap(err, "failed to get line")
		}

		t, err := parseTriplet(line)
		switch {
		case err != nil && err == errNotATriplet:
			// happens when reading non flushed file, skip
			end = end - 1
			continue
		case err != nil && err == errInvalidTriplet:
			return 0, 0, errors.Wrap(err, "failed to read line")
		}

		if t.ts > eTime {
			end = mid - 1
			continue
		}

		if abs(t.ts-eTime) <= smallestDiff {
			smallestDiff = abs(t.ts - eTime)
			if biggestTriplet == nil || t.isBigger(biggestTriplet) {
				biggestTriplet = t
			}
			begin = mid + 1
		} else {
			end = mid - 1
		}
	}
	if biggestTriplet == nil {
		// biggest item in file is less than given interval range
		return 0, 0, nil
	}

	return smallestTriplet.begin, biggestTriplet.end, nil
}

// getLine reads line where pos belongs to
// if data[pos] == '\n' - return next first
func getLine(reader io.ReadSeeker, pos int64) ([]byte, error) {
	// slower implementation, easier to code
	_, err := reader.Seek(pos, os.SEEK_SET)
	if err != nil {
		return nil, errors.Wrap(err, "failed to seek")
	}

	currPos := pos
	// read backwards and find newline or go to the begin
	ch := [1]byte{0}
	for {
		ch[0] = 0
		n, err := reader.Read(ch[:])
		if n > 0 && ch[0] == '\n' {
			break
		}

		switch {
		case err != nil && err != io.EOF:
			return nil, errors.Wrap(err, "failed to read")
		case err != nil || n == 0:
			return nil, nil
		}
		currPos--
		if currPos > -1 {
			reader.Seek(-2, os.SEEK_CUR) // 1 for above read, 1 for seek backward
		} else {
			break
		}
	}

	if currPos < 0 {
		currPos = 0
		reader.Seek(0, os.SEEK_SET)
	}

	// read forward
	buffReader := bufio.NewReader(reader)

	// assume isPrefix wouldn't be true
	line, _, err := buffReader.ReadLine()
	return line, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (tr *timeRangeReader) Read(p []byte) (n int, err error) {
	if tr.data == nil {
		return 0, io.ErrClosedPipe
	}
	if tr.begin >= tr.end {
		return 0, io.EOF
	}

	upperBound := min(tr.end-tr.begin, int64(len(p)))
	n, err = tr.data.Read(p[:upperBound])
	tr.begin = tr.begin + int64(n)
	if err == nil && tr.begin == tr.end {
		return n, io.EOF
	}
	return n, err
}

func (tr *timeRangeReader) Close() error {
	if tr.data != nil {
		tr.data.Close()
	}

	return nil
}
