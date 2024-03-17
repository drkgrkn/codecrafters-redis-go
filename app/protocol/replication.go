package protocol

import "github.com/codecrafters-io/redis-starter-go/app/utils"

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type ReplicationState struct {
	role             Role
	masterRepliID    *string
	masterReplOffset *int
}

type ReplicationStateOptFunc func(*ReplicationState)

func ReplicaOf(address string, port int) ReplicationStateOptFunc {
	return func(rs *ReplicationState) {
		rs.role = Slave
		rs.masterRepliID = nil
		rs.masterReplOffset = nil
	}
}

var replicationState ReplicationState

func InitReplicationState(opts []ReplicationStateOptFunc) error {
	repliID := utils.RandomString(40)
	repliOffset := 0
	replicationState = ReplicationState{
		role:             Master,
		masterRepliID:    &repliID,
		masterReplOffset: &repliOffset,
	}
	for _, f := range opts {
		f(&replicationState)
	}
	return nil
}

func (rs ReplicationState) Role() Role {
	return rs.role
}
func (rs ReplicationState) MasterReplicationID() *string {
	return rs.masterRepliID
}
func (rs ReplicationState) MasterReplicationOffset() *int {
	return rs.masterReplOffset
}
