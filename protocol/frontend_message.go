package protocol

import (
	"fmt"

	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/utils"
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

func BuildStartupMessage(clusterConfig *config.ClusterConfig) *StartupPgMessage {
	return &StartupPgMessage{
		ProtocolVersion: SUPPORTED_PROTOCOL_VERSION,
		User:            clusterConfig.User,
		Database:        clusterConfig.Name,
		Options:         map[string]string{"client_encoding": "UTF8", "application_name": "pgspanner"},
	}
}

// PgMessage interface implementation for StartupPgMessage
func (m *StartupPgMessage) Unpack(message *RawPgMessage) (*StartupPgMessage, error) {
	idx := 0
	idx, protocolVersion := utils.ParseInt32(message.Data, idx)
	if protocolVersion != SUPPORTED_PROTOCOL_VERSION {
		return nil, fmt.Errorf("Unsupported protocol version: %d", protocolVersion)
	}
	m.ProtocolVersion = protocolVersion

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
	idx = utils.WriteInt32(out, idx, -1)
	idx = utils.WriteInt32(out, idx, m.ProtocolVersion)
	idx = utils.WriteCString(out, idx, "user")
	idx = utils.WriteCString(out, idx, m.User)
	idx = utils.WriteCString(out, idx, "database")
	idx = utils.WriteCString(out, idx, m.Database)
	for key, value := range m.Options {
		idx, out = utils.WriteCStringSafe(out, idx, key)
		idx, out = utils.WriteCStringSafe(out, idx, value)
	}
	idx, out = utils.WriteByteSafe(out, idx, 0) // Null terminator

	// Write the actual length. We use the non safe version of WriteInt32
	// because we know the index is within the bounds of the slice
	utils.WriteInt32(out, 0, idx)

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
	idx = utils.WriteByte(out, idx, byte(FMESSAGE_QUERY))
	idx = utils.WriteInt32(out, 1, messageLength)
	utils.WriteCString(out, idx, m.Query)

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
	idx = utils.WriteByte(out, idx, byte(FMESSAGE_PASSWORD))
	idx = utils.WriteInt32(out, idx, messageLength)
	utils.WriteCString(out, idx, m.Password)

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
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteInt32(out, idx, CANCEL_REQUEST_CODE)
	idx = utils.WriteInt32(out, idx, m.BackendPid)
	idx = utils.WriteInt32(out, idx, m.BackendKey)

	return out
}

func (m *CancelRequestPgMessage) Unpack(message *RawPgMessage) (*CancelRequestPgMessage, error) {
	idx := 0

	// The body of the cancel request is 8 bytes long.
	// Determine the offset to skip the cancel request code
	offset := len(message.Data) - 8
	idx += offset // skip the rest of cancel request code
	idx, processID := utils.ParseInt32(message.Data, idx)
	idx, secretKey := utils.ParseInt32(message.Data, idx)

	return &CancelRequestPgMessage{processID, secretKey}, nil
}
