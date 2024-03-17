package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
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

func handleRequest(readWriter *bufio.ReadWriter) error {
	lead, err := nextString(readWriter)
	if err != nil {
		return err
	}
	// Parse number of arguments
	if lead[0] != '*' {
		return errors.New("Leading command string should be array")
	}
	arrLength, err := strconv.Atoi(lead[1:])
	if err != nil {
		return err
	}
	data := make([]string, arrLength)
	// Parse request command
	for i := 0; i < arrLength; i++ {
		data[i], err = parseWord(readWriter)
		if err != nil {
			return err
		}
		fmt.Printf("data in command %d, %s\n", i, data[i])
	}
	// command handling
	switch strings.ToLower(data[0]) {
	case "ping":
		err = processPingRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "echo":
		err = processEchoRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "get":
		err = processGetRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "set":
		err = processSetRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "info":
		err = processInfoRequest(readWriter, data)
		if err != nil {
			return err
		}
	}
	err = readWriter.Flush()
	if err != nil {
		return err
	}
	return readWriter.Writer.Flush()
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
