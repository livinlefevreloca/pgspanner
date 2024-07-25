package protocol

import (
	"io"
	"net"

	"github.com/livinlefevreloca/pgspanner/utils"
)

// Raw message is a low level representation of a message
// containing the kind of message, the length of the message
// and the raw bytes. Generally we read a raw message from
// a connection and then Unpack it into a higher level message
//
// RawMessage still implements the Message interface as it can
// be useful to pass it around generically
type RawMessage struct {
	Kind   int
	Length int
	Data   []byte
}

// Helper function to handle the SSL request message
func handleSSLRequest(conn net.Conn) error {
	// Read the remaining 4 bytes off the connection
	_, err := io.ReadFull(conn, make([]byte, 4))
	if err != nil {
		return err
	}
	conn.Write([]byte{78}) // send the response saying we do not support ssl at the moment
	return nil
}

func GetRawStartupMessage(conn net.Conn) (*RawMessage, error) {
	length, err := utils.ReadInt32(conn)
	if err != nil {
		return nil, err
	}

	if length == 8 {
		if err := handleSSLRequest(conn); err != nil {
			return nil, err
		}
		// Read the length again to get the actual length of the startup message
		length, err = utils.ReadInt32(conn)
	}

	messageLength := length - 4 // 4 bytes for the length

	ctxData := make([]byte, messageLength)

	_, err = io.ReadFull(conn, ctxData)
	if err != nil {
		return nil, err
	}

	return &RawMessage{FMESSAGE_STARTUP, messageLength, ctxData}, nil
}

// Reads a raw message from a connection
func GetRawMessage(conn net.Conn) (*RawMessage, error) {
	header := make([]byte, 5)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	// Read the kind of message from the header
	kind := int(header[0])

	// Read the length of the message from the header including the kind and the length itself
	_, length := utils.ParseInt32(header, 1)

	toRead := length - 4
	data := make([]byte, toRead)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	return &RawMessage{kind, length, data}, nil
}

// Message interface implementation for RawMessage
func (rm RawMessage) Unpack(data []byte) RawMessage {
	idx := 0
	kind := int(data[idx])
	idx, length := utils.ParseInt32(data, idx+1)
	data = data[idx+1:]
	return RawMessage{kind, length, data}

}

func (rm RawMessage) Pack() []byte {
	out := make([]byte, rm.Length+1)
	idx := 0

	idx = utils.WriteByte(out, idx, byte(rm.Kind))
	idx = utils.WriteInt32(out, idx+1, rm.Length)
	idx = utils.WriteBytes(out, idx, rm.Data)

	return out
}
