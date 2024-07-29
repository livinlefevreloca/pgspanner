package main

import (
	"log/slog"
	"net"

	"github.com/livinlefevreloca/pgspanner/protocol"
)

type ClientConnectionContext struct {
	DatabaseName string
	User         string
	Options      map[string]string
	Database     *DatabaseConfig
	SSL          bool
	ClientPid    int
	ClientSecret int
}

func NewClientConnectionContext(
	message *protocol.StartupPgMessage,
	database *DatabaseConfig,
	clientPid int,
) *ClientConnectionContext {
	connCtx := &ClientConnectionContext{
		User:         message.User,
		DatabaseName: message.Database,
		Options:      message.Options,
		Database:     database,
		SSL:          false,
		ClientPid:    clientPid,
		ClientSecret: clientPid,
	}
	return connCtx
}

type ClientConnection struct {
	Conn net.Conn
	Ctx  *ClientConnectionContext
}

// Implement Writer interface for ClientConnection
func (c *ClientConnection) Write(data []byte) (int, error) {
	if n, err := c.Conn.Write(data); err != nil {
		slog.Error("Error writing to client connection", "error", err)
		return n, err
	} else {
		return n, nil
	}
}

// Implement Reader interface for ClientConnection
func (c *ClientConnection) Read(data []byte) (int, error) {
	if n, err := c.Conn.Read(data); err != nil {
		slog.Error("Error reading from client connection: ", "error", err)
		return n, err
	} else {
		return n, nil
	}
}
