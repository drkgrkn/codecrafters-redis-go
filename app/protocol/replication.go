package protocol

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type ReplicationState struct {
	role Role
}

type ReplicationStateOptFunc func(*ReplicationState)

func ReplicaOf(address string, port int) ReplicationStateOptFunc {
	return func(rs *ReplicationState) {
		rs.role = Slave
	}
}

var replicationState ReplicationState

func InitReplicationState(opts []ReplicationStateOptFunc) error {
	replicationState = ReplicationState{
		role: Master,
	}
	for _, f := range opts {
		f(&replicationState)
	}
	return nil
}

func (rs ReplicationState) Role() Role {
	return rs.role
}
