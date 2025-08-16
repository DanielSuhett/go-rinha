package utils

import (
	"bytes"
	"strconv"
	"time"
)

func ExtractJSONField(data []byte, field string) string {
	fieldPattern := `"` + field + `":`
	start := bytes.Index(data, UnsafeBytes(fieldPattern))
	if start == -1 {
		return ""
	}

	start += len(fieldPattern)
	for start < len(data) && (data[start] == ' ' || data[start] == '\t') {
		start++
	}

	if start >= len(data) || data[start] != '"' {
		return ""
	}
	start++

	end := start
	for end < len(data) && data[end] != '"' {
		if data[end] == '\\' && end+1 < len(data) {
			end += 2
		} else {
			end++
		}
	}

	if end >= len(data) {
		return ""
	}

	return UnsafeString(data[start:end])
}

func ExtractAmount(data []byte) float64 {
	fieldPattern := `"amount":`
	start := bytes.Index(data, UnsafeBytes(fieldPattern))
	if start == -1 {
		return 0
	}

	start += len(fieldPattern)
	for start < len(data) && (data[start] == ' ' || data[start] == '\t') {
		start++
	}

	end := start
	for end < len(data) && (data[end] >= '0' && data[end] <= '9' || data[end] == '.') {
		end++
	}

	if end <= start {
		return 0
	}

	value, _ := strconv.ParseFloat(UnsafeString(data[start:end]), 64)
	return value
}

func ExtractRequestedAt(data []byte) string {
	return ExtractJSONField(data, "requestedAt")
}

func InjectRequestedAt(data []byte) []byte {
	if bytes.Contains(data, []byte(`"requestedAt"`)) {
		return data
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	requestedAtField := `,"requestedAt":"` + timestamp + `"`

	lastBraceIndex := bytes.LastIndexByte(data, '}')
	if lastBraceIndex == -1 {
		return data
	}

	result := make([]byte, 0, len(data)+len(requestedAtField))
	result = append(result, data[:lastBraceIndex]...)
	result = append(result, requestedAtField...)
	result = append(result, data[lastBraceIndex:]...)

	return result
}