package utils

import (
	"strconv"
	"strings"
)

// StandardizeSpaces -> Join long query to one line query
func StandardizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// GenerateSlug  -> Replace space with dash, lower it, and trim the space
func GenerateSlug(inputStr string) string {
	return strings.Trim(strings.ToLower(strings.Replace(inputStr, " ", "-", -1)), " ")
}

// String2Bool :nodoc:
func String2Bool(s string) bool {
	if s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			return i == 0
		}
	}
	return false
}

// String2Int64 :nodoc:
func String2Int64(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

// String2Int64 :nodoc:
func String2Int64WithDefault(s string, d int64) int64 {
	i := String2Int64(s)
	if i == 0 {
		return d
	}
	return i
}
