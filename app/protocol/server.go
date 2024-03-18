package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type Server struct {
	role        Role
	addr        string
	port        int
	masterState *masterConfig
	slaveState  *slaveConfig
}

type masterConfig struct {
	repliID    string
	replOffset int
}

type slaveConfig struct {
	masterAddr string
	masterPort int
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
		rs.role = Slave
		rs.masterState = nil
		rs.slaveState = &slaveConfig{
			masterAddr: address,
			masterPort: port,
		}
	}
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

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	for {
		err := s.handleRequest(rw)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Printf("client disconnected")
				return
			}
			fmt.Printf("couldn't handle request %s\n", err)
		}
	}
}

func NewServer(opts []ServerOptFunc) (*Server, error) {
	repliID := utils.RandomString(40)
	repliOffset := 0
	server := &Server{
		role: Master,
		masterState: &masterConfig{
			repliID:    repliID,
			replOffset: repliOffset,
		},
		slaveState: nil,
	}
	for _, f := range opts {
		f(server)
	}

	if server.role == Slave {
		go func() {
			err := server.handshakeMaster()
			if err != nil {
				fmt.Printf("handshake with master failed, %s", err)
			}
		}()
	}

	return server, nil
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
	defer conn.Close()
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
	return net.Dial("tcp", fmt.Sprintf("%s:%d", s.slaveState.masterAddr, s.slaveState.masterPort))
}

func (s *Server) pingMaster(rw *bufio.ReadWriter) error {
	rw.WriteString(SerializeArray(
		SerializeBulkString("PING"),
	))
	err := rw.Flush()
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
	rw.WriteString(SerializeArray(
		SerializeBulkString("REPLCONF"),
		SerializeBulkString("listening-port"),
		SerializeBulkString(fmt.Sprintf("%d", s.port)),
	))
	err := rw.Flush()
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

	rw.WriteString(SerializeArray(
		SerializeBulkString("REPLCONF"),
		SerializeBulkString("capa"),
		SerializeBulkString("psync2"),
	))
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
	rw.WriteString(SerializeArray(
		SerializeBulkString("PSYNC"),
		SerializeBulkString(fmt.Sprintf("?")),
		SerializeBulkString(fmt.Sprintf("-1")),
	))
	err := rw.Flush()
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
