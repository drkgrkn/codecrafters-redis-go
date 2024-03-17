package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func processPingRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 1 {
		return errors.New("incorrect number of arguments for the ping command")
	}
	_, err := rw.WriteString(SerializeSimpleString("PONG"))
	if err != nil {
		return err
	}
	return nil
}

func processEchoRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the echo command")
	}
	fmt.Printf("echoing \"%s\"\n", data[1])
	_, err := rw.WriteString(SerializeBulkString(data[1]))
	if err != nil {
		return err
	}
	return nil
}

func processGetRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the set command")
	}
	val, ok := store.Get(data[1])
	if !ok {
		_, err := rw.WriteString(SerializeNullBulkString())
		if err != nil {
			return err
		}
		return nil
	}

	_, err := rw.WriteString(SerializeBulkString(val))
	if err != nil {
		return err
	}
	return nil
}

func processSetRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 3 && len(data) != 5 {
		return errors.New("incorrect number of arguments for the get command")
	}

	if len(data) == 3 {
		fmt.Printf("setting key %s val %s\n", data[1], data[2])
		store.Set(data[1], data[2])
		_, err := rw.WriteString(SerializeSimpleString("OK"))
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
			store.SetWithTTL(data[1], data[2], time.Duration(dur)*time.Millisecond)
		}
		_, err := rw.WriteString(SerializeSimpleString("OK"))
		if err != nil {
			return err
		}
	}
	return nil
}

func processInfoRequest(rw *bufio.ReadWriter, data []string) error {
	if len(data) != 2 {
		return errors.New("incorrect number of arguments for the info command")
	}
	if data[1] == "replication" {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("role:%s\n", string(replicationState.Role())))
		_, err := rw.WriteString(SerializeBulkString(sb.String()))
		if err != nil {
			return err
		}
	}
	return nil
}
