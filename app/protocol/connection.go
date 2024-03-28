package protocol

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Connection struct {
	conn net.Conn
	rw   *bufio.ReadWriter

	slaveToMaster bool
}

type SlaveConnection struct {
	*Connection
	offset int
	lock   sync.Mutex
}

func NewConn(conn net.Conn, slaveToMaster bool) *Connection {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	return &Connection{
		conn,
		rw,
		slaveToMaster,
	}
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) WriteString(s string) (int, error) {
	if c.slaveToMaster {
		return 0, nil
	}
	n, err := c.rw.WriteString(s)
	if err != nil {
		return n, err
	}
	err = c.rw.Flush()
	return n, err
}

func (c *Connection) ReplyGetAck(offset int) (int, error) {
	return c.rw.WriteString(
		SerializeArray(
			SerializeBulkString("REPLCONF"),
			SerializeBulkString("ACK"),
			SerializeBulkString(fmt.Sprintf("%d", offset)),
		),
	)
}

func (c *Connection) Flush() error {
	return c.rw.Flush()
}

// returns: read string, how many bytes were read, error
func (c *Connection) nextString() (string, int, error) {
	s, err := c.rw.ReadString('\n')
	if err != nil {
		return "", 0, err
	}
	s = strings.ToLower(strings.Trim(s, "\r\n"))
	return s, len(s) + 2, nil
}

func (c *Connection) parseWord() (string, int, error) {
	readBytes := 0
	lead, n, err := c.nextString()
	if err != nil {
		return "", 0, err
	}
	readBytes += n
	switch lead[0] {
	case '+':
		s, err := DeserializeSimpleString(lead)
		return s, readBytes, err
	case '$':
		data, n, err := c.nextString()
		if err != nil {
			return "", 0, err
		}
		readBytes += n
		return DeserializeBulkString(data), readBytes, nil
	default:
		return "", 0, fmt.Errorf("unsupported starting character: %c", lead[0])
	}
}

func (c *Connection) parseCommand() (Message, error) {
	var msg Message
	lead, n, err := c.nextString()
	if err != nil {
		return msg, err
	}
	msg.readBytes += n
	// Parse number of arguments
	if lead[0] != '*' {
		return msg, fmt.Errorf("Leading command string should be array but was %s", lead)
	}
	arrLength, err := strconv.Atoi(lead[1:])
	if err != nil {
		return msg, err
	}

	msg.data = make([]string, arrLength)

	for i := 0; i < arrLength; i++ {
		msg.data[i], n, err = c.parseWord()
		if err != nil {
			return msg, err
		}
		msg.readBytes += n
		fmt.Printf("data in command %d, %s\n", i, msg.data[i])
	}
	return msg, nil
}

func (c *Connection) parseRDBFile() (string, error) {
	lead, _, err := c.nextString()
	if err != nil {
		return "", err
	}
	if lead[0] != '$' {
		return "", fmt.Errorf("expected $ symbol but got %c", lead[0])
	}
	i, err := strconv.Atoi(lead[1:])
	if err != nil {
		return "", fmt.Errorf("expected number after $ but got %s", lead[1:])
	}
	buf := make([]byte, i)
	for counter := 0; counter < i; counter++ {
		b, err := c.rw.ReadByte()
		if err != nil {
			return "", fmt.Errorf("couldn't read byte: %s", lead[1:])
		}
		buf[counter] = b
	}

	return string(buf), nil
}
