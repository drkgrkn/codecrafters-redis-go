package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/common"
)

type Role string

type Server struct {
	store *Store

	addr string
	port int

	lock         sync.Mutex
	masterConfig *masterConfig
	slaveConfig  *slaveConfig
}

type slaveConfig struct {
	addr   string
	conn   *Connection
	offset int
}

type masterConfig struct {
	id string

	offset int
	slaves []*SlaveConnection
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
		rs.slaveConfig = &slaveConfig{
			addr:   fmt.Sprintf("%s:%d", address, port),
			conn:   nil,
			offset: 0,
		}
	}
}

func NewServer(opts []ServerOptFunc) (*Server, error) {
	repliID := common.RandomString(40)
	repliOffset := 0
	server := &Server{
		store: NewStore(),
		lock:  sync.Mutex{},
		masterConfig: &masterConfig{
			id:     repliID,
			offset: repliOffset,
			slaves: []*SlaveConnection{},
		},
		slaveConfig: nil,
	}
	for _, optFunc := range opts {
		optFunc(server)
	}

	// slave server specific processes
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
		c, err := l.Accept()
		conn := NewConn(c, false)

		if err != nil {
			fmt.Println("error accepting connection: ", err)
		}
		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn *Connection) {
	for {
		err := s.handleRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				conn.conn.Close()
				fmt.Println("closing connection with client")
				break
			} else if errors.Is(err, ConnNotClientError) {
				// client is promoted to replica
				// cancel the handleClient loop
				// but do not close the connection
				fmt.Println("promoting client to slave")
				break
			}
			fmt.Printf("error with request %s\n", err)
		}
	}
}

func (s *Server) handleRequest(c *Connection) error {
	msg, err := c.nextCommand()
	if err != nil {
		return err
	}
	fmt.Println("handling command: ", msg.data)
	// command handling
	s.lock.Lock()
	defer s.lock.Unlock()
	switch strings.ToLower(msg.data[0]) {
	case "ping":
		err = s.processPingRequest(c, msg)
	case "echo":
		err = s.processEchoRequest(c, msg)
	case "get":
		err = s.processGetRequest(c, msg)
	case "set":
		err = s.processSetRequest(c, msg)
	case "info":
		err = s.processInfoRequest(c, msg)
	case "replconf":
		err = s.processReplConfRequest(c, msg)
	case "psync":
		err := s.processPsyncRequest(c, msg)
		fmt.Println("post psync req ", err)
		if err == nil {
			return ConnNotClientError
		}
	case "wait":
		err = s.processWaitRequest(c, msg)
	}
	// replicas should update their offset for all propogations from the master
	if c.slaveToMaster {
		s.incrementOffset(msg.readBytes)
	}
	fmt.Println("handled command: ", msg.data)
	return err
}

func (s *Server) incrementOffset(i int) {
	s.slaveConfig.offset += i
}

// handshake goes as:
//
// - slave sends PING to master and receivers PONG
//
// - slave sends REPLCONF twice to the master
//
// - slave sends PSYNC to the master
func (s *Server) handshakeMaster() error {
	c, err := net.Dial("tcp", fmt.Sprintf("%s", s.slaveConfig.addr))
	conn := NewConn(c, true)
	s.slaveConfig.conn = conn
	if err != nil {
		return err
	}

	err = s.pingMaster(conn)
	if err != nil {
		return fmt.Errorf("error while pinging master: %w", err)
	}

	err = s.configureReplicationWithMaster(conn)
	if err != nil {
		return fmt.Errorf("error while configuring replication with master: %w", err)
	}

	err = s.psyncWithMaster(conn)
	if err != nil {
		return fmt.Errorf("error while configuring replication with master: %w", err)
	}

	return nil
}

func (s *Server) pingMaster(c *Connection) error {
	_, err := c.rw.WriteString(SerializeArray(
		SerializeBulkString("PING"),
	))
	if err != nil {
		return err
	}
	err = c.rw.Flush()
	if err != nil {
		return err
	}
	resp, _, err := c.nextString()
	if err != nil {
		return fmt.Errorf("master didn't response to ping: %w", err)
	}
	pong, err := DeserializeSimpleString(resp)
	if err != nil || strings.ToLower(pong) != "pong" {
		return fmt.Errorf("expected master to reply pong got %s", pong)
	}
	return nil
}

func (s *Server) configureReplicationWithMaster(c *Connection) error {
	_, err := c.rw.WriteString(SerializeArray(
		SerializeBulkString("REPLCONF"),
		SerializeBulkString("listening-port"),
		SerializeBulkString(fmt.Sprintf("%d", s.port)),
	))
	if err != nil {
		return err
	}
	err = c.rw.Flush()
	if err != nil {
		return err
	}
	resp, _, err := c.nextString()
	if err != nil {
		return fmt.Errorf("master didn't respond to REPLCONF: %w", err)
	}
	ok, err := DeserializeSimpleString(resp)
	if err != nil || strings.ToLower(ok) != "ok" {
		return fmt.Errorf("expected master to reply ok got %s", ok)
	}

	_, err = c.rw.WriteString(SerializeArray(
		SerializeBulkString("REPLCONF"),
		SerializeBulkString("capa"),
		SerializeBulkString("psync2"),
	))
	if err != nil {
		return err
	}
	err = c.rw.Flush()
	if err != nil {
		return err
	}
	resp, _, err = c.nextString()
	if err != nil {
		return fmt.Errorf("master didn't respond to REPLCONF: %w", err)
	}
	ok, err = DeserializeSimpleString(resp)
	if err != nil || strings.ToLower(ok) != "ok" {
		return fmt.Errorf("expected master to reply ok got %s", ok)
	}

	return nil
}

func (s *Server) psyncWithMaster(c *Connection) error {
	_, err := c.rw.WriteString(SerializeArray(
		SerializeBulkString("PSYNC"),
		SerializeBulkString(fmt.Sprintf("?")),
		SerializeBulkString(fmt.Sprintf("-1")),
	))
	if err != nil {
		return err
	}
	err = c.rw.Flush()
	if err != nil {
		return err
	}
	resp, _, err := c.nextString()
	if err != nil {
		return fmt.Errorf("master didn't respond to REPLCONF: %w", err)
	}
	_, err = DeserializeSimpleString(resp)

	_, err = c.parseRDBFile()
	if err != nil {
		return fmt.Errorf("expected rdbfile but %w", err)
	}

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
	if s.masterConfig == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	propagationCmd := SerializeArray(
		SerializeBulkString("SET"),
		SerializeBulkString(key),
		SerializeBulkString(val),
	)
	s.masterConfig.offset += len(propagationCmd)
	for _, c := range s.masterConfig.slaves {
		wg.Add(1)
		go func(sc *SlaveConnection) {
			defer wg.Done()
			sc.lock.Lock()
			defer sc.lock.Unlock()

			command := fmt.Sprintf("\"%s %s %s\"", "SET", key, val)
			addr := sc.conn.RemoteAddr().String()
			fmt.Printf("syncing with slave %s, command: %s\n", addr, command)

			_, err := sc.WriteString(propagationCmd)
			if err != nil {
				fmt.Printf(
					"failure while propagating %s command to replica %s, error: %s",
					command, addr, err)
				return
			}

			fmt.Printf("synced with slave %s, command %s\n", addr, command)
		}(c)
	}
	wg.Wait()
	fmt.Println("SET FINISHED")
	return nil
}

// `s *Server` should be locked when this function is called
//
// channel sends when a slave's offset is equal to master's
func (s *Server) SyncSlaves(ctx context.Context) <-chan int {
	var (
		fanInChan = make(chan int, len(s.masterConfig.slaves))
		ch        = make(chan int, len(s.masterConfig.slaves))
	)
	s.masterConfig.offset += len(CommandReplConfGetAck)
	for _, sc := range s.masterConfig.slaves {
		sc := sc
		go sc.Sync(ctx, fanInChan)
	}

	go func() {
		inSyncCount := 0
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			case offset := <-fanInChan:
				fmt.Printf("got offset %d, master is %d\n", offset, s.masterConfig.offset)
				if offset == s.masterConfig.offset {
					inSyncCount++
					ch <- inSyncCount
				}
			}
		}
	}()

	return ch
}
