package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	l, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConn(conn)
	}
}

type ConnectionCtx struct {
	database        string
	user            string
	clientEncoding  string
	applicationName string
	options         []string
	protocolVersion int
}

type RawMessage struct {
	kind   int
	Length int
	Data   []byte
}

func getRawMessage(conn net.Conn) (RawMessage, error) {
	var message RawMessage
	header := make([]byte, 5)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return message, err
	}

	// Read the kind of message from the header
	message.kind = int(header[0])
	// Read the length of the message from the header including the kind and the length itself
	message.Length = int(binary.BigEndian.Uint32(header[1:5]))

	toRead := message.Length - 4 // 4 bytes for the length
	message.Data = make([]byte, toRead)
	_, err = io.ReadFull(conn, message.Data)
	if err != nil {
		return message, err
	}

	return message, nil
}

func handleSSLRequest(conn net.Conn) error {
	// Read the remaining 4 bytes off the connection
	_, err := io.ReadFull(conn, make([]byte, 4))
	if err != nil {
		return err
	}
	conn.Write([]byte{78}) // send the response saying we do not support ssl at the moment
	return nil
}

func handleStartupRequestWrapper(conn net.Conn, connCtx *ConnectionCtx) error {
	length, err := readInt32(conn)
	if err != nil {
		return err
	}
	return handleStartupRequest(conn, length, connCtx)
}

func handleStartupRequest(conn net.Conn, length int, connCtx *ConnectionCtx) error {
	protocolVersion, err := readInt32(conn)
	connCtx.protocolVersion = protocolVersion
	fmt.Println("Protocol version: ", protocolVersion)
	if err != nil {
		return err
	}
	messageLength := length - 8 // 4 bytes for the length and 4 bytes for the protocol version
	fmt.Println("Message length: ", messageLength)

	ctxData := make([]byte, messageLength)

	_, err = io.ReadFull(conn, ctxData)
	if err != nil {
		return err
	}
	// Parse the context data
	parseStartupMessage(ctxData, connCtx)

	return nil
}

func parseOptions(options string) []string {
	return nil
}

func parseStartupMessage(data []byte, connCtx *ConnectionCtx) {
	idx := 0
	nextIdx := idx

	for idx < len(data) {
		nextIdx = idx + bytes.IndexByte(data[idx:], 0)
		field := string(data[idx:nextIdx])
		fmt.Println("Field: ", field)
		idx = nextIdx + 1
		nextIdx = idx + bytes.IndexByte(data[idx:], 0)

		switch field {
		case "user":
			connCtx.user = string(data[idx:nextIdx])
			fmt.Println("User: ", connCtx.user)
			idx = nextIdx + 1
			break
		case "database":
			connCtx.database = string(data[idx:nextIdx])
			fmt.Println("Database: ", connCtx.database)
			idx = nextIdx + 1
			break
		case "client_encoding":
			connCtx.clientEncoding = string(data[idx:nextIdx])
			fmt.Println("Client encoding: ", connCtx.clientEncoding)
			idx = nextIdx + 1
			break
		case "application_name":
			connCtx.applicationName = string(data[idx:nextIdx])
			fmt.Println("Application name: ", connCtx.applicationName)
			idx = nextIdx + 1
			break
		case "options":
			options := string(data[idx:nextIdx])
			connCtx.options = parseOptions(options)
			fmt.Println("Got Options")
			idx = nextIdx + 1
			break
		default:
			// if we do not recognize the field, we skip it
			idx = nextIdx + 1
		}

	}

}

func handleConn(conn net.Conn) {
	defer conn.Close()
	length, err := readInt32(conn)
	if err != nil {
		panic(err)
	}
	fmt.Println("Length of the message: ", length)

	var connCtx ConnectionCtx
	if length == 8 {
		fmt.Println("SSL Request")
		handleSSLRequest(conn)
		fmt.Println("Startup Request")
		handleStartupRequestWrapper(conn, &connCtx)
	} else {
		fmt.Println("Startup Request")
		handleStartupRequest(conn, length, &connCtx)
	}

	fmt.Println("Sending AuthenticationOk and ReadyForQuery messages")
	// Send AuthenticationOk message followed by the ReadyForQuery message
	conn.Write([]byte{82, 0, 0, 0, 8, 0, 0, 0, 0, 90, 00, 00, 00, 05, 73})
	fmt.Println("Connection established with user: ", connCtx.user, " and database: ", connCtx.database)

	conn.Read(make([]byte, 1))
	fmt.Println("Client closed the connection")
}

func readInt32(reader io.Reader) (int, error) {
	var value int32
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return int(value), nil
}
