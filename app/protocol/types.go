package protocol

import (
	"fmt"
)

func DeserializeSimpleString(s string) (string, error) {
	ret := s[1 : len(s)-2]
	return ret, nil
}
func SerializeSimpleString(s string) string {
	v := fmt.Sprintf("_%s\r\n", s)
	return v
}
func DeserializeSimpleError(s string) (string, error) {
	ret := s[1 : len(s)-2]
	return ret, nil
}
func SerializeSimpleError(s string) string {
	v := fmt.Sprintf("-%s\r\n", s)
	return v
}
func DeserializeBulkString(data string) string {
	return data
}
func SerializeBulkString(s string) string {
	v := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	return v
}
