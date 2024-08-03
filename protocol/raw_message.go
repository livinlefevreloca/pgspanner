package protocol

import (
	"bytes"
	"io"
	"log/slog"
	"net"

	"github.com/livinlefevreloca/pgspanner/protocol/parsing"
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
	length, err := parsing.ReadInt32(conn)
	if err != nil {
		return nil, err
	}

	if length == 8 {
		slog.Info("SSL request received")
		if err := handleSSLRequest(conn); err != nil {
			return nil, err
		}
		// Read the length again to get the actual length of the startup message
		length, err = parsing.ReadInt32(conn)
	} else if length == 16 {
		// This is a cancel request
		data := make([]byte, 12)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			slog.Error("Error reading cancel request in startup message")
			return nil, err
		}
		return &RawPgMessage{FMESSAGE_CANCEL, 16, data}, nil
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
func GetRawPgMessage(conn io.Reader) (*RawPgMessage, error) {
	header := make([]byte, 5)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	// This is a cancel request
	if bytes.Equal(header, []byte{0x00, 0x00, 0x00, 0x10, 0x04}) {
		data := make([]byte, 11)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			slog.Error("Error reading cancel request")
			return nil, err
		}
		return &RawPgMessage{FMESSAGE_CANCEL, 16, data}, nil
	}

	// Read the kind of message from the header
	kind := int(header[0])

	// Read the length of the message from the header including the kind and the length itself
	_, length := parsing.ParseInt32(header, 1)

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
	idx, length := parsing.ParseInt32(data, idx+1)
	data = data[idx+1:]

	return RawPgMessage{kind, length, data}
}

func (rm RawPgMessage) Pack() []byte {
	out := make([]byte, rm.Length+1)

	idx := 0
	idx = parsing.WriteByte(out, idx, byte(rm.Kind))
	idx = parsing.WriteInt32(out, idx, rm.Length)
	parsing.WriteBytes(out, idx, rm.Data)

	return out
}
