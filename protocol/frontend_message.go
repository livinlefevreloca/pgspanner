package protocol

import (
	"fmt"

	"github.com/livinlefevreloca/pgspanner/utils"
)

// Frontend Message kinds
const (
	FMESSAGE_STARTUP   = -1
	FMESSAGE_QUERY     = 81
	FMESSAGE_TERMINATE = 88
)

// StartupMessage represents the message sent by the client to start the connection
type StartupMessage struct {
	ProtocolVersion int
	User            string
	Database        string
	Replication     string
	Options         map[string]string
}

// Message interface implementation for StartupMessage
func (m *StartupMessage) Unpack(message *RawMessage) (*StartupMessage, error) {
	idx := 0
	idx, protocolVersion := utils.ParseInt32(message.Data, idx)

	m.ProtocolVersion = protocolVersion
	fmt.Println("Protocol Version: ", m.ProtocolVersion)

	options := make(map[string]string)

	remaining := len(message.Data) - idx

	var key, value string
	var err error
	for idx < remaining {
		idx, key, err = utils.ParseCString(message.Data, idx)
		if err != nil {
			return nil, err
		}
		idx, value, err = utils.ParseCString(message.Data, idx)
		if err != nil {
			return nil, err
		}
		switch key {
		case "user":
			m.User = value
		case "database":
			m.Database = value
		case "replication":
			m.Replication = value
		default:
			options[key] = value
		}
	}

	m.Options = options

	return m, nil
}

// QueryMessage represents the message sent by the client to query the database
type QueryMessage struct {
	Query string
}

// Message interface implementation for QueryMessage
func (m *QueryMessage) Unpack(message *RawMessage) (*QueryMessage, error) {
	return &QueryMessage{string(message.Data)}, nil
}

func (m QueryMessage) Pack() []byte {
	queryBytes := []byte(m.Query)
	messageLength := 4 + len(queryBytes) + 1 // length + query + null terminator

	out := make([]byte, messageLength)
	idx := 0

	idx = utils.WriteByte(out, idx, byte(FMESSAGE_QUERY))
	idx = utils.WriteInt32(out, 1, messageLength)
	_ = utils.WriteCString(out, idx, queryBytes)

	return out
}
