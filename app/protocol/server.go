package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

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
	conn net.Conn
}

type masterConfig struct {
	repliID    string
	replOffset int
	lock       sync.Mutex
	slaves     []net.Conn
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
			conn: nil,
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
			lock:       sync.Mutex{},
			slaves:     []net.Conn{},
		},
		slaveConfig: nil,
	}
	for _, f := range opts {
		f(server)
	}

	if server.slaveConfig != nil {
		err := server.handshakeMaster()
		if err != nil {
			fmt.Printf("handshake with master failed, %s", err)
		}
	}

	return server, nil
}

func (s *Server) Listen() error {
	if s.slaveConfig != nil {
		go s.handleClient(s.slaveConfig.conn)
	}
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.addr, s.port))
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err)
		}
		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) error {
	defer func() {
		conn.Close()
		fmt.Println("closing connection with client")
	}()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	for {
		err := s.handleRequest(conn, rw)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("client disconnected")
			}
			fmt.Printf("error with request %s\n", err)
		}
	}
}

func (s *Server) handleRequest(conn net.Conn, rw *bufio.ReadWriter) error {
	lead, err := nextString(rw)
	if err != nil {
		return err
	}
	// Parse number of arguments
	if lead[0] != '*' {
		return fmt.Errorf("Leading command string should be array but was %s", lead)
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
	case "echo":
		err = s.processEchoRequest(rw, data)
	case "get":
		err = s.processGetRequest(rw, data)
	case "set":
		err = s.processSetRequest(rw, data)
	case "info":
		err = s.processInfoRequest(rw, data)
	case "replconf":
		err = s.processReplConfRequest(rw, data)
	case "psync":
		err = s.processPsyncRequest(rw, data, conn)
	}
	return err
}

// handshake goes as:
//
// - slave sends PING to master and receivers PONG
//
// - slave sends REPLCONF twice to the master
//
// - slave sends PSYNC to the master
func (s *Server) handshakeMaster() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s", s.slaveConfig.addr))
	s.slaveConfig.conn = conn
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

	rdbFile, err := parseWord(rw)
	if err != nil {
		return fmt.Errorf("expected rdbfile but %s", err)
	}
	_, err = DeserializeSimpleString(rdbFile)

	return nil
}

// Sets given key to val
//
// # If the server is master, also propagates the command to replicas
//
// Setting operation itself cannot fail, not-nil error means
// at least one replica failed during propagation
func (s *Server) Set(key, val string) error {
	s.store.Set(key, val)
	if s.masterConfig != nil {
		propagationCmd := (SerializeArray(
			SerializeBulkString("SET"),
			SerializeBulkString(key),
			SerializeBulkString(val),
		))
		s.masterConfig.lock.Lock()
		defer s.masterConfig.lock.Unlock()
		for _, conn := range s.masterConfig.slaves {
			conn := conn
			command := fmt.Sprintf("\"%s %s %s\"", "SET", key, val)
			addr := conn.RemoteAddr().String()
			fmt.Printf("syncing with slave %s, command: %s\n", addr, command)

			rw := common.ReadWriterFrom(conn)
			_, err := rw.WriteString(propagationCmd)
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
			fmt.Printf("synced with slave %s, command %s\n", addr, command)
		}
	}

	return nil
}
