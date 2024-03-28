package protocol

import (
	"fmt"
	"strconv"
)

type Message struct {
	data      []string
	readBytes int
}

func (m Message) parseReplConfAck() (int, error) {
	if len(m.data) != 3 {
		return 0, fmt.Errorf("repl conf ack length should be 3, got %d", len(m.data))
	}
	if m.data[0] != "replconf" {
		return 0, fmt.Errorf("repl conf ack first word should be 'replconf', got %s", m.data[0])
	}
	if m.data[1] != "ack" {
		return 0, fmt.Errorf("repl conf ack second word should be 'ack', got %s", m.data[1])
	}
	offset, err := strconv.Atoi(m.data[2])
	if err != nil {
		return 0, fmt.Errorf("repl conf ack third word should be a number but %w", err)
	}
	return offset, nil
}
