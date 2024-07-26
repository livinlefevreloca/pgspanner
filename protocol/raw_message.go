package protocol

import (
	"io"
	"net"

	"github.com/livinlefevreloca/pgspanner/utils"
)

// RawPgMessage is a low level representation of a message
// containing the kind of message, the length of the message
// and the raw bytes. Generally we read a raw message from
// a connection and then Unpack it into a higher level message
//
// RawPgMessage still implements the Message interface as it can
// be useful to pass it around generically
type RawPgMessage struct {
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

func GetRawStartupPgMessage(conn net.Conn) (*RawPgMessage, error) {
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

	return &RawPgMessage{FMESSAGE_STARTUP, messageLength, ctxData}, nil
}

// Reads a raw message from a connection
func GetRawPgMessage(conn net.Conn) (*RawPgMessage, error) {
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
	if toRead == 0 {
		return &RawPgMessage{kind, length, data}, nil
	}
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	return &RawPgMessage{kind, length, data}, nil
}

// Message interface implementation for RawPgMessage
func (rm RawPgMessage) Unpack(data []byte) RawPgMessage {
	idx := 0
	kind := int(data[idx])
	idx, length := utils.ParseInt32(data, idx+1)
	data = data[idx+1:]

	return RawPgMessage{kind, length, data}
}

func (rm RawPgMessage) Pack() []byte {
	out := make([]byte, rm.Length+1)

	idx := 0
	idx = utils.WriteByte(out, idx, byte(rm.Kind))
	idx = utils.WriteInt32(out, idx, rm.Length)
	utils.WriteBytes(out, idx, rm.Data)

	return out
}
