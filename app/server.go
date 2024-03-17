package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"

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
	flag.Parse()

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("failed to bind to port ", port)
		os.Exit(1)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err.Error())
		}
		go handleClient(conn)
	}

}

func handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Println("closing connection with client")
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	for {
		err := protocol.HandleRequest(rw)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Printf("client disconnected")
				return
			}
			fmt.Printf("couldn't handle request %s\n", err)
		}
	}
}

func WritePong(conn net.Conn) (int, error) {
	pong := []byte("+PONG\r\n")
	fmt.Println("writing pong")
	return conn.Write(pong)
}
