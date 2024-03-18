package protocol

import (
	"bufio"
	"fmt"
	"strings"
)

func nextString(readWriter *bufio.ReadWriter) (string, error) {
	s, err := readWriter.ReadString('\n')
	if err != nil {
		return "", err
	}
	s = strings.Trim(s, "\r\n")
	return s, nil
}

func parseWord(readWriter *bufio.ReadWriter) (string, error) {
	lead, err := nextString(readWriter)
	if err != nil {
		return "", err
	}
	switch lead[0] {
	case '+':
		return DeserializeSimpleString(lead)
	case '$':
		data, err := nextString(readWriter)
		if err != nil {
			return "", err
		}
		return DeserializeBulkString(data), nil
	default:
		return "", fmt.Errorf("unsupported starting character: %c", lead[0])
	}
}
