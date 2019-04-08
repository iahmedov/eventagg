package server

import (
	"fmt"
)

type Error struct {
	Key, Value string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s - %s", e.Key, e.Value)
}

func newError(key, value string) *Error {
	return &Error{
		Key:   key,
		Value: value,
	}
}
