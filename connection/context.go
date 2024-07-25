package connection

import "github.com/livinlefevreloca/pgspanner/protocol"

type ConnectionContext struct {
	Database        string
	User            string
	ClientEncoding  string
	ApplicationName string
	Options         map[string]string
	ProtocolVersion int
	ssl             bool
}

func NewConnectionContext(message *protocol.StartupMessage) *ConnectionContext {
	connCtx := &ConnectionContext{
		ProtocolVersion: message.ProtocolVersion,
		User:            message.User,
		Database:        message.Database,
		ClientEncoding:  message.Options["client_encoding"],
		ApplicationName: message.Options["application_name"],
		Options:         message.Options,
		ssl:             false,
	}
	return connCtx
}
