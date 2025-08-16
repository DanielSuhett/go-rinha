package utils

import "unsafe"

func UnsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}