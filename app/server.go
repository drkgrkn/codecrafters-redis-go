package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
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
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		wg.Add(1)
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		go handleClient(conn, &wg)
	}

}

func handleClient(conn net.Conn, wg *sync.WaitGroup) {
	defer conn.Close()
	defer wg.Done()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println("error while reading: ", err)
			}
		}

		ins := strings.Split(string(buf[:n]), "\n")
		for range ins {
			_, err := WritePong(conn)
			if err != nil {
				fmt.Println("error while writing: ", err)
			}
		}
	}
}

func WritePong(conn net.Conn) (int, error) {
	pong := []byte("+PONG\r\n")
	fmt.Println("writing pong")
	return conn.Write(pong)
}
