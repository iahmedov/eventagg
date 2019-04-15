package cold

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type triplet struct {
	begin, end, ts int64
}

var (
	errNotATriplet    = errors.New("not a triplet")
	errInvalidTriplet = errors.New("invalid triplet format")
)

func parseTriplet(data []byte) (*triplet, error) {
	str := string(data)
	splitted := strings.Split(str, ",")
	if len(splitted) != 3 {
		return nil, errNotATriplet
	}

	begin, err := strconv.ParseInt(splitted[0], 10, 64)
	if err != nil {
		return nil, errInvalidTriplet
	}

	end, err := strconv.ParseInt(splitted[1], 10, 64)
	if err != nil {
		return nil, errInvalidTriplet
	}

	ts, err := strconv.ParseInt(splitted[2], 10, 64)
	if err != nil {
		return nil, errInvalidTriplet
	}

	return &triplet{begin, end, ts}, nil
}

func (t1 *triplet) isSmaller(t2 *triplet) bool {
	if t2 == nil {
		return false
	}

	if t1.ts == t2.ts {
		if t1.begin < t2.begin {
			return true
		}
		return false
	}

	return t1.ts < t2.ts
}

func (t1 *triplet) isBigger(t2 *triplet) bool {
	if t2 == nil {
		return false
	}

	if t1.ts == t2.ts {
		if t1.begin > t2.begin {
			return true
		}
		return false
	}

	return t1.ts > t2.ts
}
