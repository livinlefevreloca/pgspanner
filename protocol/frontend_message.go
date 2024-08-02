package protocol

import (
	"fmt"

	"github.com/livinlefevreloca/pgspanner/protocol/parsing"
)

/// An implementation of the Postgres protocol messages that are sent by the client to the server
/// Described in detail https://www.postgresql.org/docs/current/protocol-message-formats.html

// Frontend PgMessage kinds
const (
	FMESSAGE_STARTUP   = -1
	FMESSAGE_QUERY     = 81
	FMESSAGE_TERMINATE = 88
	FMESSAGE_PASSWORD  = 112
	FMESSAGE_CANCEL    = -2
	FMESSAGE_SASL      = 112
)

const (
	SUPPORTED_PROTOCOL_VERSION = 196608
)

// StartupPgMessage represents the message sent by the client to start the connection
type StartupPgMessage struct {
	ProtocolVersion int
	User            string
	Database        string
	Options         map[string]string
}

func BuildStartupMessage(user string, database string) *StartupPgMessage {
	return &StartupPgMessage{
		ProtocolVersion: SUPPORTED_PROTOCOL_VERSION,
		User:            user,
		Database:        database,
		Options:         map[string]string{"client_encoding": "UTF8", "application_name": "pgspanner"},
	}
}

// PgMessage interface implementation for StartupPgMessage
func (m *StartupPgMessage) Unpack(message *RawPgMessage) (*StartupPgMessage, error) {
	idx := 0
	idx, protocolVersion := parsing.ParseInt32(message.Data, idx)
	if protocolVersion != SUPPORTED_PROTOCOL_VERSION {
		return nil, fmt.Errorf("Unsupported protocol version: %d", protocolVersion)
	}
	m.ProtocolVersion = protocolVersion

	options := make(map[string]string)
	remaining := len(message.Data) - idx

	var key, value string
	var err error
	for idx < remaining {
		idx, key, err = parsing.ParseCString(message.Data, idx)
		if err != nil {
			return nil, err
		}
		idx, value, err = parsing.ParseCString(message.Data, idx)
		if err != nil {
			return nil, err
		}
		switch key {
		case "user":
			m.User = value
		case "database":
			m.Database = value
		default:
			options[key] = value
		}
	}

	m.Options = options

	return m, nil
}

func (m StartupPgMessage) Pack() []byte {
	out := make([]byte, 1024) // 1KB should be enough for the startup message
	idx := 0

	// Write a dummy length for now
	idx = parsing.WriteInt32(out, idx, -1)
	idx = parsing.WriteInt32(out, idx, m.ProtocolVersion)
	idx = parsing.WriteCString(out, idx, "user")
	idx = parsing.WriteCString(out, idx, m.User)
	idx = parsing.WriteCString(out, idx, "database")
	idx = parsing.WriteCString(out, idx, m.Database)
	for key, value := range m.Options {
		idx, out = parsing.WriteCStringSafe(out, idx, key)
		idx, out = parsing.WriteCStringSafe(out, idx, value)
	}
	idx, out = parsing.WriteByteSafe(out, idx, 0) // Null terminator

	// Write the actual length. We use the non safe version of WriteInt32
	// because we know the index is within the bounds of the slice
	parsing.WriteInt32(out, 0, idx)

	return out[:idx]
}

// QueryPgMessage represents the message sent by the client to query the database
type QueryPgMessage struct {
	Query string
}

func BuildQueryMessage(query string) *QueryPgMessage {
	return &QueryPgMessage{query}
}

// PgMessage interface implementation for QueryPgMessage
func (m *QueryPgMessage) Unpack(message *RawPgMessage) (*QueryPgMessage, error) {
	return &QueryPgMessage{string(message.Data[:len(message.Data)-1])}, nil
}

func (m QueryPgMessage) Pack() []byte {
	messageLength := 4 + len(m.Query) + 1 // length + query + null terminator

	out := make([]byte, messageLength+1)

	idx := 0
	idx = parsing.WriteByte(out, idx, byte(FMESSAGE_QUERY))
	idx = parsing.WriteInt32(out, 1, messageLength)
	parsing.WriteCString(out, idx, m.Query)

	return out
}

// PasswordPgMessage represents the message sent by the client to authenticate
type PasswordPgMessage struct {
	Password string
}

func BuildPasswordMessage(password string) *PasswordPgMessage {
	return &PasswordPgMessage{password}
}

// PgMessage interface implementation for PasswordPgMessage
func (m *PasswordPgMessage) Unpack(message *RawPgMessage) (*PasswordPgMessage, error) {
	return &PasswordPgMessage{string(message.Data[:len(message.Data)-1])}, nil
}

func (m PasswordPgMessage) Pack() []byte {
	messageLength := 4 + len(m.Password) + 1 //  length + password + null terminator

	out := make([]byte, messageLength+1)

	idx := 0
	idx = parsing.WriteByte(out, idx, byte(FMESSAGE_PASSWORD))
	idx = parsing.WriteInt32(out, idx, messageLength)
	parsing.WriteCString(out, idx, m.Password)

	return out
}

const (
	CANCEL_REQUEST_CODE int = 80877102
)

// CancelRequestPgMessage represents the message sent by the client to cancel a query
type CancelRequestPgMessage struct {
	BackendPid int
	BackendKey int
}

func BuildCancelRequestPgMessage(backendPid int, backendKey int) *CancelRequestPgMessage {
	return &CancelRequestPgMessage{backendPid, backendKey}
}

// PgMessage interface implementation for CancelRequestPgMessage
func (m *CancelRequestPgMessage) Pack() []byte {
	messageLength := 16 // length + cancelRequestCode + processID + secretKey
	out := make([]byte, messageLength)

	idx := 0
	idx = parsing.WriteInt32(out, idx, messageLength)
	idx = parsing.WriteInt32(out, idx, CANCEL_REQUEST_CODE)
	idx = parsing.WriteInt32(out, idx, m.BackendPid)
	idx = parsing.WriteInt32(out, idx, m.BackendKey)

	return out
}

func (m *CancelRequestPgMessage) Unpack(message *RawPgMessage) (*CancelRequestPgMessage, error) {
	idx := 0

	// The body of the cancel request is 8 bytes long.
	// Determine the offset to skip the cancel request code
	offset := len(message.Data) - 8
	idx += offset // skip the rest of cancel request code
	idx, processID := parsing.ParseInt32(message.Data, idx)
	idx, secretKey := parsing.ParseInt32(message.Data, idx)

	return &CancelRequestPgMessage{processID, secretKey}, nil
}

// SASLInitialResponsePgMessage represents the message sent by the client to authenticate using SASL
type SASLInitialResponsePgMessage struct {
	Mechanism string
	Response  []byte
}

func BuildSASLInitialResponseMessage(mechanism string, response []byte) *SASLInitialResponsePgMessage {
	return &SASLInitialResponsePgMessage{mechanism, response}
}

// PgMessage interface implementation for SASLInitialResponsePgMessage
func (m *SASLInitialResponsePgMessage) Unpack(message *RawPgMessage) (*SASLInitialResponsePgMessage, error) {
	idx := 0
	idx, mechanism, err := parsing.ParseCString(message.Data, idx)
	if err != nil {
		return nil, err
	}
	idx, length := parsing.ParseInt32(message.Data, idx)
	idx, response, err := parsing.ParseBytes(message.Data, idx, int(length))
	if err != nil {
		return nil, err
	}

	return &SASLInitialResponsePgMessage{mechanism, response}, nil
}

func (m SASLInitialResponsePgMessage) Pack() []byte {
	messageLength := 4 + len(m.Mechanism) + 1 + 4 + len(m.Response) // length + mechanism + null terminator + response length + response

	out := make([]byte, messageLength+1)

	idx := 0
	idx = parsing.WriteByte(out, idx, byte(FMESSAGE_SASL))
	idx = parsing.WriteInt32(out, idx, messageLength)
	idx = parsing.WriteCString(out, idx, m.Mechanism)
	idx = parsing.WriteInt32(out, idx, len(m.Response))
	parsing.WriteBytes(out, idx, m.Response)

	return out
}

// SASLResponsePgMessage represents the message sent by the client to authenticate using SASL
type SASLResponsePgMessage struct {
	Response []byte
}

func BuildSASLResponseMessage(response []byte) *SASLResponsePgMessage {
	return &SASLResponsePgMessage{response}
}

// PgMessage interface implementation for SASLResponsePgMessage
func (m *SASLResponsePgMessage) Unpack(message *RawPgMessage) (*SASLResponsePgMessage, error) {
	idx := 0
	idx, length := parsing.ParseInt32(message.Data, idx)
	idx, response, err := parsing.ParseBytes(message.Data, idx, int(length))
	if err != nil {
		return nil, err
	}

	return &SASLResponsePgMessage{response}, nil
}

func (m SASLResponsePgMessage) Pack() []byte {
	messageLength := 4 + len(m.Response) // length + response

	out := make([]byte, messageLength+1)

	idx := 0
	idx = parsing.WriteByte(out, idx, byte(FMESSAGE_SASL))
	idx = parsing.WriteInt32(out, idx, messageLength)
	parsing.WriteBytes(out, idx, m.Response)

	return out
}
