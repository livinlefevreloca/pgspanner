package client

import (
	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/protocol"
)

type ClientConnectionContext struct {
	DatabaseName string
	User         string
	Options      map[string]string
	Database     *config.DatabaseConfig
	SSL          bool
}

func NewClientConnectionContext(
	message *protocol.StartupPgMessage,
	database *config.DatabaseConfig,
) *ClientConnectionContext {
	connCtx := &ClientConnectionContext{
		User:         message.User,
		DatabaseName: message.Database,
		Options:      message.Options,
		Database:     database,
		SSL:          false,
	}
	return connCtx
}
