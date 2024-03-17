package protocol

import (
	"bufio"
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type ReplicationState struct {
	role        Role
	masterState *masterState
	slaveState  *slaveState
}

type masterState struct {
	repliID    string
	replOffset int
}

type slaveState struct {
	masterAddr string
	masterPort int
}

type ReplicationStateOptFunc func(*ReplicationState)

func ReplicaOf(address string, port int) ReplicationStateOptFunc {
	return func(rs *ReplicationState) {
		rs.role = Slave
		rs.masterState = nil
		rs.slaveState = &slaveState{
			masterAddr: address,
			masterPort: port,
		}
	}
}

var replicationState ReplicationState

func InitReplicationState(opts []ReplicationStateOptFunc) error {
	repliID := utils.RandomString(40)
	repliOffset := 0
	replicationState = ReplicationState{
		role: Master,
		masterState: &masterState{
			repliID:    repliID,
			replOffset: repliOffset,
		},
		slaveState: nil,
	}
	for _, f := range opts {
		f(&replicationState)
	}

	if replicationState.role == Slave {
		err := replicationState.pingMaster()
		if err != nil {
			return fmt.Errorf("couldn't ping master, %s", err)
		}
	}

	return nil
}

func (rs *ReplicationState) pingMaster() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", rs.slaveState.masterAddr, rs.slaveState.masterPort))
	if err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	rw.WriteString(SerializeArray(
		SerializeBulkString("PING"),
	))
	err = rw.Flush()
	if err != nil {
		return err
	}
	return rw.Writer.Flush()
}
