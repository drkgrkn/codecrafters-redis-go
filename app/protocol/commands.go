package protocol

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (s *Server) processPingRequest(c *Connection, msg Message) error {
	if len(msg.data) != 1 {
		return errors.New("incorrect number of arguments for the ping command")
	}

	_, err := c.WriteString(SerializeSimpleString("PONG"))
	if err != nil {
		return err
	}
	err = c.Flush()
	return err
}

func (s *Server) processEchoRequest(c *Connection, msg Message) error {
	if len(msg.data) != 2 {
		return errors.New("incorrect number of arguments for the echo command")
	}

	fmt.Printf("echoing \"%s\"\n", msg.data[1])
	_, err := c.WriteString(SerializeBulkString(msg.data[1]))
	if err != nil {
		return err
	}
	err = c.Flush()
	return err
}

func (s *Server) processGetRequest(c *Connection, msg Message) error {
	if len(msg.data) != 2 {
		return errors.New("incorrect number of arguments for the set command")
	}

	key := msg.data[1]
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

func (s *Server) processSetRequest(c *Connection, msg Message) error {
	if len(msg.data) != 3 && len(msg.data) != 5 {
		return errors.New("incorrect number of arguments for the get command")
	}

	if len(msg.data) == 3 {
		fmt.Printf("setting key %s val %s\n", msg.data[1], msg.data[2])
		err := s.Set(msg.data[1], msg.data[2])
		if err != nil {
			fmt.Printf("error while propogating set command: %s", err)
		}
		_, err = c.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	} else if len(msg.data) == 5 {
		if strings.ToLower(msg.data[3]) == "px" {
			dur, err := strconv.Atoi(msg.data[4])
			if err != nil {
				return err
			}
			fmt.Printf("setting key %s val %s for %d ms\n", msg.data[1], msg.data[2], dur)
			s.store.SetWithTTL(msg.data[1], msg.data[2], time.Duration(dur)*time.Millisecond)
		}
		_, err := c.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	}
	err := c.Flush()
	return err
}

func (s *Server) processInfoRequest(c *Connection, msg Message) error {
	if len(msg.data) != 2 {
		return errors.New("incorrect number of arguments for the info command")
	}
	if msg.data[1] == "replication" {
		var sb strings.Builder
		if s.masterConfig != nil {
			sb.WriteString(fmt.Sprintf("role:%s\n", "master"))
			sb.WriteString(fmt.Sprintf("master_replid:%s\n", s.masterConfig.id))
			sb.WriteString(fmt.Sprintf("master_repl_offset:%d\n", s.masterConfig.offset))
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

func (s *Server) processReplConfRequest(c *Connection, msg Message) error {
	// only slaves receive getack from master to assure consistency
	if len(msg.data) != 3 {
		return errors.New("incorrect number of arguments for the replconf command")
	}

	switch strings.ToLower(msg.data[1]) {
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

func (s *Server) processPsyncRequest(c *Connection, msg Message) error {
	if len(msg.data) != 3 {
		return errors.New("incorrect number of arguments for the psync command")
	}
	_, err := c.WriteString(
		SerializeSimpleString(
			fmt.Sprintf("FULLRESYNC %s %d", s.masterConfig.id, s.masterConfig.offset),
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

	s.masterConfig.slaves = append(s.masterConfig.slaves, &SlaveConnection{
		Connection: c,
		offset:     0,
		lock:       sync.Mutex{},
	})

	return nil
}

func (s *Server) processWaitRequest(c *Connection, msg Message) error {
	ctx := context.Background()

	if len(msg.data) != 3 {
		return errors.New("incorrect number of arguments for the wait command")
	}
	reqInSyncReplCount, err := strconv.Atoi(msg.data[1])
	if err != nil {
		return fmt.Errorf("wait command second arg should be integer but %w", err)
	}
	ms, err := strconv.Atoi(msg.data[2])
	if err != nil {
		return fmt.Errorf("wait command third arg should be integer but %w", err)
	}
	ctx, ctxCancel := context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
	defer ctxCancel()

	currInSyncCount := 0
	for _, sc := range s.masterConfig.slaves {
		fmt.Printf("master offset %d replica offset is %d\n", s.masterConfig.offset, sc.offset)
		if s.masterConfig.offset == sc.offset {
			currInSyncCount++
		}
	}
	fmt.Printf("%d replicas are currently in sync\n", currInSyncCount)
	if currInSyncCount >= reqInSyncReplCount || len(s.masterConfig.slaves) == currInSyncCount {
		fmt.Printf("enough replicas are in sync for wait command\n")
		_, err = c.WriteString(SerializeInteger(currInSyncCount))
		return err
	}
	fmt.Printf("resyncing with slaves\n")
	ch := s.SyncSlaves(ctx)
	inSyncCount := 0
	for {
		select {
		case <-ctx.Done():
			_, _ = c.WriteString(SerializeInteger(inSyncCount))
			return ctx.Err()
		case <-ch:
			inSyncCount++
			if inSyncCount == reqInSyncReplCount {
				_, err = c.WriteString(SerializeInteger(inSyncCount))
				return err
			}
		}
	}
}
