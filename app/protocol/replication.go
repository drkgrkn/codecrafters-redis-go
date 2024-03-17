package protocol

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type ReplicationState struct {
	role Role
}

var replicationState ReplicationState = ReplicationState{
	role: Master,
}

func (rs ReplicationState) Role() Role {
	return rs.role
}
