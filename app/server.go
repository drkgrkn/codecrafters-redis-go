package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

const (
	CommandECHO = "echo"
	CommandPING = "ping"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("logs from your program will appear here!")

	port := flag.Int("port", 6379, "port of the instance")
	masterAddr := flag.String("replicaof", "-1", "address of the master")
	flag.Parse()
	server, err := initServer("0.0.0.0", *port, masterAddr)
	if err != nil {
		log.Fatalf("couldn't initialize server: %s", err)
	}

	if err = server.Listen(); err != nil {
		log.Fatal(fmt.Errorf("error while listening %s", err))
	}
}

func initServer(addr string, port int, masterAddr *string) (*protocol.Server, error) {
	rsOpts := []protocol.ServerOptFunc{
		protocol.ListenOn(addr, port),
	}

	// is this instance a replica
	if *masterAddr != "-1" {
		args := os.Args
		replicaOfIdx := slices.IndexFunc(args, func(arg string) bool {
			return arg == *masterAddr
		})
		// replicaOfIdx cannot be -1 as masterAddr is not -1
		// no need to check

		if len(args) <= replicaOfIdx+1 {
			return nil, errors.New("not enough arguments were given")
		}
		masterPortStr := args[replicaOfIdx+1]
		masterPort, err := strconv.Atoi(masterPortStr)
		if err != nil {
			return nil, fmt.Errorf("given master port is invalid: %s", err)
		}
		rsOpts = append(rsOpts, protocol.ReplicaOf(*masterAddr, masterPort))
	}

	return protocol.NewServer(rsOpts)
}
