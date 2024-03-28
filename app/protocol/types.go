package protocol

import (
	"fmt"
	"strings"
)

type unit struct{}

func DeserializeSimpleString(s string) (string, error) {
	ret := s[1:]
	return ret, nil
}
func SerializeSimpleString(s string) string {
	v := fmt.Sprintf("+%s\r\n", s)
	return v
}
func DeserializeSimpleError(s string) (string, error) {
	ret := s[1:]
	return ret, nil
}
func SerializeSimpleError(s string) string {
	v := fmt.Sprintf("-%s\r\n", s)
	return v
}

func SerializeInteger(i int) string {
	var symbol string
	if i >= 0 {
		symbol = "+"
	} else {
		symbol = "-"
	}
	return fmt.Sprintf(":%s%d\r\n", symbol, i)
}

func DeserializeBulkString(data string) string {
	return data
}
func SerializeBulkString(s string) string {
	v := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	return v
}
func SerializeNullBulkString() string {
	return "$-1\r\n"
}
func SerializeArray(elements ...string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(elements)))
	for _, str := range elements {
		sb.WriteString(str)
	}
	return sb.String()
}
