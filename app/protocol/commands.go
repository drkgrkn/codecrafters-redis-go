package protocol

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (s *Server) processPingRequest(c *Connection, data []string) error {
	if len(data) != 1 {
		return errors.New("incorrect number of arguments for the ping command")
	}

	_, err := c.WriteString(SerializeSimpleString("PONG"))
	if err != nil {
		return err
	}
	err = c.Flush()
	return err
}

func (s *Server) processEchoRequest(c *Connection, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the echo command")
	}

	fmt.Printf("echoing \"%s\"\n", data[1])
	_, err := c.WriteString(SerializeBulkString(data[1]))
	if err != nil {
		return err
	}
	err = c.Flush()
	return err
}

func (s *Server) processGetRequest(c *Connection, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the set command")
	}

	key := data[1]
	val, ok := s.store.Get(key)
	if !ok {
		fmt.Printf("key %s does not exist\n", key)
		_, err := c.WriteString(SerializeNullBulkString())
		if err != nil {
			return err
		}
	} else {
		fmt.Printf("key %s exists, value %s\n", key, val)
		_, err := c.WriteString(SerializeBulkString(val))
		if err != nil {
			return err
		}
	}
	err := c.Flush()
	return err
}

func (s *Server) processSetRequest(c *Connection, data []string) error {
	if len(data) != 3 && len(data) != 5 {
		return errors.New("incorrect number of arguments for the get command")
	}

	if len(data) == 3 {
		fmt.Printf("setting key %s val %s\n", data[1], data[2])
		err := s.Set(data[1], data[2])
		if err != nil {
			fmt.Printf("error while propogating set command: %s", err)
		}
		_, err = c.WriteString(SerializeSimpleString("OK"))
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
		_, err := c.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	}
	err := c.Flush()
	return err
}

func (s *Server) processInfoRequest(c *Connection, data []string) error {
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
		_, err := c.WriteString(SerializeBulkString(sb.String()))
		if err != nil {
			return err
		}
	}
	err := c.Flush()
	return err
}

func (s *Server) processReplConfRequest(c *Connection, data []string) error {
	if len(data) != 3 {
		return errors.New("incorrect number of arguments for the replconf command")
	}

	switch strings.ToLower(data[1]) {
	// only slaves receive getack from master to assure consistency
	case "getack":
		if s.slaveConfig == nil {
			return errors.New("non-master should not receive getack")
		}
		_, err := c.ReplyGetAck(s.slaveConfig.offset)
		if err != nil {
			return err
		}
	default:
		_, err := c.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	}

	err := c.Flush()
	return err
}

func (s *Server) processPsyncRequest(c *Connection, data []string) error {
	if len(data) != 3 {
		return errors.New("incorrect number of arguments for the psync command")
	}
	_, err := c.WriteString(
		SerializeSimpleString(
			fmt.Sprintf("FULLRESYNC %s %d", s.masterConfig.repliID, s.masterConfig.replOffset),
		),
	)
	if err != nil {
		return err
	}

	_, err = c.WriteString(strings.TrimSuffix(SerializeBulkString(getEmptyRDBFileBinary()), "\r\n"))
	if err != nil {
		return err
	}
	err = c.Flush()
	if err != nil {
		return err
	}

	s.masterConfig.lock.Lock()
	defer s.masterConfig.lock.Unlock()
	s.masterConfig.slaves = append(s.masterConfig.slaves, c)

	return nil
}
