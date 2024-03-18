package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type Server struct {
	role         Role
	addr         string
	port         int
	masterConfig *masterConfig
	slaveConfig  *slaveConfig
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
		rs.masterConfig = nil
		rs.slaveConfig = &slaveConfig{
			masterAddr: address,
			masterPort: port,
		}
	}
}

func NewServer(opts []ServerOptFunc) (*Server, error) {
	repliID := utils.RandomString(40)
	repliOffset := 0
	server := &Server{
		role: Master,
		masterConfig: &masterConfig{
			repliID:    repliID,
			replOffset: repliOffset,
		},
		slaveConfig: nil,
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

func (s *Server) handleRequest(readWriter *bufio.ReadWriter) error {
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
		err = s.processPingRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "echo":
		err = s.processEchoRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "get":
		err = s.processGetRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "set":
		err = s.processSetRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "info":
		err = s.processInfoRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "replconf":
		err = s.processReplConfRequest(readWriter, data)
		if err != nil {
			return err
		}
	case "psync":
		err = s.processPsyncRequest(readWriter, data)
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
	return net.Dial("tcp", fmt.Sprintf("%s:%d", s.slaveConfig.masterAddr, s.slaveConfig.masterPort))
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
