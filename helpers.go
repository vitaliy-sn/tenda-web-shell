package main

import (
	"encoding/json"
	"strings"
)

func jsonUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func joinLines(lines []string) string {
	return strings.Join(lines, "\r\n")
}
