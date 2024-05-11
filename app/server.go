package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Printf("Listening on %v", l.Addr())

	defer l.Close()

	for {
		go handleConnectionConcurrently(l)
	}
}

func handleConnectionConcurrently(l net.Listener) {
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}

	handleClient(conn)
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		log.Printf("Received %d bytes", n)
		log.Printf("Received the following data: %s", string(buf[:n]))

		message := []byte("+PONG\r\n")
		n, err = conn.Write(message)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}

		log.Printf("Sent %d bytes", n)
	}
}
