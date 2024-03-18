package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func (s *Server) processPingRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 1 {
		return errors.New("incorrect number of arguments for the ping command")
	}
	_, err := rw.WriteString(SerializeSimpleString("PONG"))
	if err != nil {
		return err
	}
	err = rw.Flush()
	return err
}

func (s *Server) processEchoRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the echo command")
	}
	fmt.Printf("echoing \"%s\"\n", data[1])
	_, err := rw.WriteString(SerializeBulkString(data[1]))
	if err != nil {
		return err
	}
	err = rw.Flush()
	return err
}

func (s *Server) processGetRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the set command")
	}
	val, ok := s.store.Get(data[1])
	if !ok {
		_, err := rw.WriteString(SerializeNullBulkString())
		if err != nil {
			return err
		}
	} else {
		_, err := rw.WriteString(SerializeBulkString(val))
		if err != nil {
			return err
		}
	}
	err := rw.Flush()
	return err
}

func (s *Server) processSetRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 3 && len(data) != 5 {
		return errors.New("incorrect number of arguments for the get command")
	}

	if len(data) == 3 {
		fmt.Printf("setting key %s val %s\n", data[1], data[2])
		err := s.Set(data[1], data[2])
		if err != nil {
			fmt.Printf("error while propogating set command: %s", err)
		}
		_, err = rw.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	} else if len(data) == 5 {
		if strings.ToLower(data[3]) == "px" {
			dur, err := strconv.Atoi(data[4])
			if err != nil {
				return err
			}
			fmt.Printf("setting key %s val %s for %d ms\n", data[1], data[2], dur)
			s.store.SetWithTTL(data[1], data[2], time.Duration(dur)*time.Millisecond)
		}
		_, err := rw.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	}
	err := rw.Flush()
	return err
}

func (s *Server) processInfoRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the info command")
	}
	if data[1] == "replication" {
		var sb strings.Builder
		if s.masterConfig != nil {
			sb.WriteString(fmt.Sprintf("role:%s\n", "master"))
			sb.WriteString(fmt.Sprintf("master_replid:%s\n", s.masterConfig.repliID))
			sb.WriteString(fmt.Sprintf("master_repl_offset:%d\n", s.masterConfig.replOffset))
		} else {
			sb.WriteString(fmt.Sprintf("role:%s\n", "slave"))
		}
		_, err := rw.WriteString(SerializeBulkString(sb.String()))
		if err != nil {
			return err
		}
	}
	err := rw.Flush()
	return err
}

func (s *Server) processReplConfRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 3 {
		return errors.New("incorrect number of arguments for the replconf command")
	}

	switch strings.ToLower(data[1]) {
	// only slaves receive getack from master to assure consistency
	case "getack":
		if s.slaveConfig == nil {
			return errors.New("non-master should not receive getack")
		}
		_, err := rw.WriteString(
			SerializeArray(
				SerializeBulkString("REPLCONF"),
				SerializeBulkString("ACK"),
				SerializeBulkString(fmt.Sprintf("%d", s.slaveConfig.offset)),
			),
		)
		if err != nil {
			return err
		}
	default:
		_, err := rw.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	}

	err := rw.Flush()
	return err
}

func (s *Server) processPsyncRequest(rw *bufio.ReadWriter, data []string, conn net.Conn) error {
	if len(data) != 3 {
		return errors.New("incorrect number of arguments for the psync command")
	}
	_, err := rw.WriteString(
		SerializeSimpleString(
			fmt.Sprintf("FULLRESYNC %s %d", s.masterConfig.repliID, s.masterConfig.replOffset),
		),
	)
	if err != nil {
		return err
	}

	_, err = rw.WriteString(strings.TrimSuffix(SerializeBulkString(getEmptyRDBFileBinary()), "\r\n"))
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}

	s.masterConfig.lock.Lock()
	defer s.masterConfig.lock.Unlock()
	s.masterConfig.slaves = append(s.masterConfig.slaves, conn)

	return nil
}
