package cold

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	pfile "github.com/iahmedov/eventagg/pkg/persistence/file"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestFindTimeRange(t *testing.T) {
	appFS := afero.NewMemMapFs()
	// create test files and directories

	ranges := []string{
		"1,2,99",
		"2,10,102",
		"10,11,102",
		"12,13,103",
		"14,16,104",
		"14,18,105",
		"18,20,106",
	}
	data := []byte(strings.Join(ranges, "\n"))
	require.NoError(t, afero.WriteFile(appFS, "/testing.idx", data, 0777))
	fread, err := appFS.OpenFile("/testing.idx", os.O_RDWR, 0777)
	defer fread.Close()
	require.NoError(t, err)
	require.NotNil(t, fread)

	b, e, err := findTimeRange(fread, 101, 105, int64(len(data)))
	require.NoError(t, err)
	require.EqualValues(t, 2, b)
	require.EqualValues(t, 18, e)

	b, e, err = findTimeRange(fread, 90, 98, int64(len(data)))
	require.NoError(t, err)
	require.EqualValues(t, 0, b)
	require.EqualValues(t, 0, e)

	b, e, err = findTimeRange(fread, 107, 110, int64(len(data)))
	require.NoError(t, err)
	require.EqualValues(t, 0, b)
	require.EqualValues(t, 0, e)
}

func TestFindInBrokenFileTimeRange(t *testing.T) {
	appFS := afero.NewMemMapFs()
	// create test files and directories

	ranges := []string{
		"1,2,99",
		"2,10,102",
		"10,11,102",
		"12,13,103",
		"14,15",
	}
	data := []byte(strings.Join(ranges, "\n"))
	require.NoError(t, afero.WriteFile(appFS, "/testing.idx", data, 0777))
	fread, err := appFS.OpenFile("/testing.idx", os.O_RDWR, 0777)
	defer fread.Close()
	require.NoError(t, err)
	require.NotNil(t, fread)

	b, e, err := findTimeRange(fread, 102, 110, int64(len(data)))
	require.NoError(t, err)
	require.EqualValues(t, 2, b)
	require.EqualValues(t, 13, e)

	b, e, err = findTimeRange(fread, 90, 110, int64(len(data)))
	require.NoError(t, err)
	require.EqualValues(t, 1, b)
	require.EqualValues(t, 13, e)

	b, e, err = findTimeRange(fread, 90, 102, int64(len(data)))
	require.NoError(t, err)
	require.EqualValues(t, 1, b)
	require.EqualValues(t, 11, e)
}

func TestRangeReader(t *testing.T) {
	appFS := afero.NewOsFs()
	// create test files and directories

	ranges := []string{
		"1,2,99",
		"2,10,102",
		"10,11,102",
		"12,13,103",
		"14,16,104",
		"16,18,105",
		"18,20,106",
	}
	content := []byte("abcdefghijklmnopqrstuvwxyz")
	dataDir := "/tmp"
	dataFile := pfile.DataFilePath(dataDir)
	indexData := []byte(strings.Join(ranges, "\n"))
	indexFile := pfile.IndexFilePath(dataDir)

	require.NoError(t, afero.WriteFile(appFS, indexFile, indexData, 0777))
	require.NoError(t, afero.WriteFile(appFS, dataFile, content, 0777))

	time.Now().Unix()

	// time
	readCloser, err := newTimeRangeReader(dataDir, unixToTime(90), unixToTime(105))
	require.NoError(t, err)
	defer readCloser.Close()

	content, err = ioutil.ReadAll(readCloser)
	require.NoError(t, err)
	// range to read: 1-18
	require.Equal(t, []byte("bcdefghijklmnopqr"), content)
}
