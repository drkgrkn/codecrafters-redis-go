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

func HandleRequest(readWriter *bufio.ReadWriter) error {
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
		if len(data) != 1 {
			return errors.New("incorrect number of arguments for the ping command")
		}
		_, err = readWriter.WriteString(SerializeSimpleString("PONG"))
		if err != nil {
			return err
		}
	case "echo":
		if len(data) != 2 {
			return errors.New("incorrect number of arguments for the echo command")
		}
		fmt.Printf("echoing \"%s\"\n", data[1])
		_, err = readWriter.WriteString(SerializeBulkString(data[1]))
		if err != nil {
			return err
		}
	case "get":
		if len(data) != 2 {
			return errors.New("incorrect number of arguments for the set command")
		}
		val, ok := store.Get(data[1])
		if !ok {
			_, err = readWriter.WriteString(SerializeBulkString("-1"))
			if err != nil {
				return err
			}
		}
		_, err = readWriter.WriteString(SerializeBulkString(val))
	case "set":
		if len(data) != 3 {
			return errors.New("incorrect number of arguments for the get command")
		}
		store.Set(data[1], data[2])
		_, err = readWriter.WriteString(SerializeSimpleString("OK"))
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
