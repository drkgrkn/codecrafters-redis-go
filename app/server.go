package main

import (
	"fmt"
	"io"
	"net"
	"os"
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

	buf := make([]byte, 1024)
main:
	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println("error while reading: ", err)
				break main
			}
		}

		_, err = WritePong(conn)
		if err != nil {
			fmt.Println("error while writing: ", err)
			break main
		}
	}
}

func WritePong(conn net.Conn) (int, error) {
	pong := []byte("+PONG\r\n")
	fmt.Println("writing pong")
	return conn.Write(pong)
}
