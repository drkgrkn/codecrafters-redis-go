package protocol

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Connection struct {
	conn net.Conn
	rw   *bufio.ReadWriter
	lock sync.Mutex

	slaveToMaster bool
}

type SlaveConnection struct {
	*Connection
	offset int
}

func NewConn(conn net.Conn, slaveToMaster bool) *Connection {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	return &Connection{
		conn:          conn,
		rw:            rw,
		lock:          sync.Mutex{},
		slaveToMaster: slaveToMaster,
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
	n, err := c.rw.WriteString(
		SerializeArray(
			SerializeBulkString("REPLCONF"),
			SerializeBulkString("ACK"),
			SerializeBulkString(fmt.Sprintf("%d", offset)),
		),
	)
	if err != nil {
		return n, err
	}
	err = c.rw.Flush()
	return n, err
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

func (c *Connection) nextCommand() (Message, error) {
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
	}
	fmt.Printf("incoming: %s\n", msg.data)
	return msg, nil
}

func (c *Connection) parseRDBFile() (string, error) {
	lead, _, err := c.nextString()
	if err != nil {
		return "", err
	}
	if lead[0] != '$' {
		return "", fmt.Errorf("expected symbol $ but got %c", lead[0])
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

func (sc *SlaveConnection) Sync(ctx context.Context, ch chan<- int) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	_, err := sc.WriteString(CommandReplConfGetAck)
	if err != nil {
		return
	}

	cmdChan := make(chan Message)
	go func() {
		defer close(cmdChan)
		msg, err := sc.nextCommand()
		if err != nil {
			fmt.Printf("%s\n", err)
		}
		cmdChan <- msg
	}()
	select {
	case <-ctx.Done():
		return
	case msg, ok := <-cmdChan:
		if !ok {
			return
		}
		offset, err := msg.parseReplConfAck()
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}

		fmt.Printf("received offset %d\n", offset)
		sc.offset = offset
		ch <- offset
	}

}
