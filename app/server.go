package main

import (
	"fmt"
	"strings"
	// "log"
	"net"
	"os"

	resp "github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Client struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Printf("Listening on %v\n", l.Addr())

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
	client := &Client{
		conn: conn,
		reader: resp.NewReader(conn),
		writer: resp.NewWriter(conn),	
	} 

	defer client.conn.Close()

	for {
		result, err := client.reader.Read()
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		content, err := result.Slice()
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}

		fmt.Println("Content received:", content)
		command := content[0].(string)

		switch strings.ToUpper(command) {
		case "ECHO":
			err = client.writer.WriteBulkString([]byte(content[1].(string)))
		default:
			err = client.writer.WriteBulkString([]byte("PONG"))
		}

		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
	}
}
