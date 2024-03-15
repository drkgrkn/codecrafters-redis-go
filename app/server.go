package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
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
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}
	handleClient(conn)
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		s := bufio.NewScanner(conn)
		s.Split(bufio.ScanLines)
		for s.Scan() {
			_ = s.Text()
			_, err := WritePong(conn)
			if err != nil {
				fmt.Println("error while reading from client ", s.Err())
				return
			}
		}
		if s.Err() == io.EOF {
			_ = s.Text()
			WritePong(conn)
		} else if s.Err() != nil {
			fmt.Println("error while reading from client ", s.Err())
			return
		}
	}
}

func WritePong(conn net.Conn) (int, error) {
	pong := []byte("+PONG\r\n")
	return conn.Write(pong)
}
