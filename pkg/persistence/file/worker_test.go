package file

import (
	// "io/ioutil"
	"os"
	"testing"

	"github.com/iahmedov/eventagg"

	"github.com/stretchr/testify/require"
)

func TestWorker(t *testing.T) {
	// fl, err := ioutil.TempFile("", "eventagg-worker")
	// require.NoError(t, err)
	// defer os.Remove(fl.Name())

	w, err := newWorker(os.TempDir())
	require.NoError(t, err)
	defer w.Close()

	for i := 0; i < 10; i++ {
		require.NoError(t, w.Add(&eventagg.Event{
			Time: int64(i),
		}))
	}
}
