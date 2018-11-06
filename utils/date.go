package utils

import (
	"time"
)

// DateToString :nodoc:
func DateToString(t time.Time) string {
	s := t.Format(time.RFC3339)
	return s
}
