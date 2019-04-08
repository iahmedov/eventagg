package file

import (
	"github.com/iahmedov/eventagg"
)

func fanoutRoundRobin(in chan *eventagg.Event, out ...chan *eventagg.Event) {
	go func() {
		if len(out) == 0 {
			return
		}

		currentIdx := 0
		for ev := range in {
			out[currentIdx] <- ev
			currentIdx++
			currentIdx = currentIdx % len(out)
		}

		for _, ch := range out {
			close(ch)
		}
	}()
}
