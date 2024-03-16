package main

import (
	"bufio"
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
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		go handleClient(conn)
	}

}

func handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Println("Closing connection with client")
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(r, w)
	for {
		err := protocol.HandleRequest(rw)
		if err != nil {
			if err == io.EOF {
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
