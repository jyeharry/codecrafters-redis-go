package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	resp "github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Client struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
}

var DB = NewDatabase()

const (
	GET  = "GET"
	SET  = "SET"
	PING = "PING"
	ECHO = "ECHO"
	PX = "PX" 
)

var sem = make(chan int, 100)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Printf("Listening on %v\n", l.Addr())

	defer l.Close()
	
	for {
		sem <- 1
		go handleConcurrentConnections(l)
	}
}

func handleConcurrentConnections(l net.Listener) {
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}

	handleClient(conn)
}

func handleClient(conn net.Conn) {
	client := &Client{
		conn:   conn,
		reader: resp.NewReader(conn),
		writer: resp.NewWriter(conn),
	}

	defer func() {
		client.conn.Close()
		<- sem
	}()

	for {
		result, err := client.reader.Read()
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		content, err := result.Slice()
		if err != nil {
			fmt.Println("Error reading from result: ", err.Error())
			os.Exit(1)
		}

		fmt.Printf("Content received: %s\n", content)

		command := content[0].(string)
		args := content[1:]

		switch strings.ToUpper(command) {
		case ECHO:
			echo := args[0].(string)
			err = client.writer.WriteBulkString([]byte(echo))
		case SET:
			key := args[0].(string)
			flags := make(map[string]string)
			remainingArgs := args[2:]
			for i := 0; i < len(remainingArgs); i += 2 {
				flag := remainingArgs[i].(string)
				flags[strings.ToUpper(flag)] = remainingArgs[i+1].(string) 
			}
			expiry, parseIntErr := strconv.ParseUint(flags[PX], 10, 64)
			if parseIntErr != nil {
				expiry = 0
			}
			DB.Set(key, args[1].(string), uint(expiry))
			err = client.writer.WriteSimpleString([]byte("OK"))
		case GET:
			key := args[0].(string)
			if value, ok := DB.Get(key); ok {
				err = client.writer.WriteBulkString([]byte(value))
			} else {
				err = client.writer.WriteNil()
			}
		case PING:
			err = client.writer.WriteBulkString([]byte("PONG"))
		default:
			fmt.Printf("Command not registered: %s", command)
		}

		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
	}
}
