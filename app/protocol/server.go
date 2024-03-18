package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/common"
)

type Role string

type Server struct {
	store *Store

	addr string
	port int

	masterConfig *masterConfig
	slaveConfig  *other
}

type other struct {
	addr string
}

type masterConfig struct {
	repliID    string
	replOffset int
	slaves     []*net.Conn
}

type ServerOptFunc func(*Server)

func WithAddressAndPort(address string, port int) ServerOptFunc {
	return func(rs *Server) {
		rs.addr = address
		rs.port = port
	}
}
func WithMasterAs(address string, port int) ServerOptFunc {
	return func(rs *Server) {
		rs.masterConfig = nil
		rs.slaveConfig = &other{
			addr: fmt.Sprintf("%s:%d", address, port),
		}
	}
}

func NewServer(opts []ServerOptFunc) (*Server, error) {
	repliID := common.RandomString(40)
	repliOffset := 0
	server := &Server{
		store: NewStore(),
		masterConfig: &masterConfig{
			repliID:    repliID,
			replOffset: repliOffset,
		},
		slaveConfig: nil,
	}
	for _, f := range opts {
		f(server)
	}

	if server.slaveConfig != nil {
		go func() {
			err := server.handshakeMaster()
			if err != nil {
				fmt.Printf("handshake with master failed, %s", err)
			}
		}()
	}

	return server, nil
}

func (s *Server) Listen() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.addr, s.port))
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err.Error())
		}
		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Println("closing connection with client")
	}()
	for {
		err := s.handleRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("client disconnected")
				return
			}
			fmt.Printf("error with request %s\n", err)
		}
	}
}

func (s *Server) handleRequest(conn net.Conn) error {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	lead, err := nextString(rw)
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
		data[i], err = parseWord(rw)
		if err != nil {
			return err
		}
		fmt.Printf("data in command %d, %s\n", i, data[i])
	}
	// command handling
	switch strings.ToLower(data[0]) {
	case "ping":
		err = s.processPingRequest(rw, data)
		if err != nil {
			return err
		}
	case "echo":
		err = s.processEchoRequest(rw, data)
		if err != nil {
			return err
		}
	case "get":
		err = s.processGetRequest(rw, data)
		if err != nil {
			return err
		}
	case "set":
		err = s.processSetRequest(rw, data)
		if err != nil {
			return err
		}
	case "info":
		err = s.processInfoRequest(rw, data)
		if err != nil {
			return err
		}
	case "replconf":
		err = s.processReplConfRequest(rw, data)
		if err != nil {
			return err
		}
	case "psync":
		err = s.processPsyncRequest(rw, data, conn)
		if err != nil {
			return err
		}
	}
	err = rw.Flush()
	if err != nil {
		return err
	}
	return rw.Writer.Flush()
}

// handshake goes as:
//
// - slave sends PING to master and receivers PONG
//
// - slave sends REPLCONF twice to the master
//
// - slave sends PSYNC to the master
func (s *Server) handshakeMaster() error {
	conn, err := s.dial()
	if err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)

	err = s.pingMaster(rw)
	if err != nil {
		return fmt.Errorf("error while pinging master: %s", err)
	}
	err = s.configureReplicationWithMaster(rw)
	if err != nil {
		return fmt.Errorf("error while configuring replication with master: %s", err)
	}
	err = s.psyncWithMaster(rw)
	if err != nil {
		return fmt.Errorf("error while configuring replication with master: %s", err)
	}

	return nil
}

func (s *Server) dial() (net.Conn, error) {
	return net.Dial("tcp", fmt.Sprintf("%s", s.slaveConfig.addr))
}

func (s *Server) pingMaster(rw *bufio.ReadWriter) error {
	_, err := rw.WriteString(SerializeArray(
		SerializeBulkString("PING"),
	))
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}
	resp, err := nextString(rw)
	if err != nil {
		return fmt.Errorf("master didn't response to ping: %s", err)
	}
	pong, err := DeserializeSimpleString(resp)
	if err != nil || strings.ToLower(pong) != "pong" {
		return fmt.Errorf("expected master to reply pong got %s", pong)
	}
	return nil
}

func (s *Server) configureReplicationWithMaster(rw *bufio.ReadWriter) error {
	_, err := rw.WriteString(SerializeArray(
		SerializeBulkString("REPLCONF"),
		SerializeBulkString("listening-port"),
		SerializeBulkString(fmt.Sprintf("%d", s.port)),
	))
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}
	resp, err := nextString(rw)
	if err != nil {
		return fmt.Errorf("master didn't respond to REPLCONF: %s", err)
	}
	ok, err := DeserializeSimpleString(resp)
	if err != nil || strings.ToLower(ok) != "ok" {
		return fmt.Errorf("expected master to reply ok got %s", ok)
	}

	_, err = rw.WriteString(SerializeArray(
		SerializeBulkString("REPLCONF"),
		SerializeBulkString("capa"),
		SerializeBulkString("psync2"),
	))
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}
	resp, err = nextString(rw)
	if err != nil {
		return fmt.Errorf("master didn't respond to REPLCONF: %s", err)
	}
	ok, err = DeserializeSimpleString(resp)
	if err != nil || strings.ToLower(ok) != "ok" {
		return fmt.Errorf("expected master to reply ok got %s", ok)
	}

	return nil
}

func (s *Server) psyncWithMaster(rw *bufio.ReadWriter) error {
	_, err := rw.WriteString(SerializeArray(
		SerializeBulkString("PSYNC"),
		SerializeBulkString(fmt.Sprintf("?")),
		SerializeBulkString(fmt.Sprintf("-1")),
	))
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}
	resp, err := nextString(rw)
	if err != nil {
		return fmt.Errorf("master didn't respond to REPLCONF: %s", err)
	}
	_, err = DeserializeSimpleString(resp)

	return nil
}

func (s *Server) Set(key, val string) error {
	s.store.Set(key, val)
	if s.masterConfig != nil {
		for _, conn := range s.masterConfig.slaves {
			command := fmt.Sprintf("\"%s %s %s\"", "SET", key, val)
			addr := (*conn).RemoteAddr().String()
			fmt.Printf("syncing with slave %s, command: %s\n", addr, command)

			rw := common.ReadWriterFrom(*conn)
			_, err := rw.WriteString(SerializeArray(
				SerializeBulkString("SET"),
				SerializeBulkString(key),
				SerializeBulkString(val),
			))
			if err != nil {
				return fmt.Errorf(
					"failure while propagating %s command to replica %s, error: %s",
					command, addr, err)
			}

			err = rw.Flush()
			if err != nil {
				return fmt.Errorf(
					"failure while propagating %s command to replica %s, error: %s",
					command, addr, err)
			}
		}
	}

	return nil
}
