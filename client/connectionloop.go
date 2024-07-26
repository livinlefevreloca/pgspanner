package client

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/server"
	"github.com/livinlefevreloca/pgspanner/utils"
)

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

func staticServerConfiguration(ctx *ClientConnectionContext) *map[string]string {
	return &map[string]string{
		"application_name":            "psql",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
		"IntervalStyle":               "postgres",
		"is_superuser":                "on",
		"server_encoding":             "UTF8",
		"server_version":              "pgspanner-0.0.1",
		"session_authorization":       ctx.User,
		"standard_conforming_strings": "on",
		"TimeZone":                    "UTC",
	}
}

// group the auth and config messages into one write
func configPacketShim(ctx *ClientConnectionContext) []byte {
	buffer := make([]byte, 1024)
	idx := 0
	serverConfig := staticServerConfiguration(ctx)

	// For now just send an authentication ok message
	authPgMessage := protocol.BuildAuthenticationOkPgMessage()
	idx = utils.WriteBytes(buffer, idx, authPgMessage.Pack())
	for k, v := range *serverConfig {
		configPgMessage := protocol.BuildParameterStatusPgMessage(k, v)
		idx = utils.WriteBytes(buffer, idx, configPgMessage.Pack())
	}
	keyDataPgMessage := protocol.BuildBackendKeyDataPgMessage(1337, 1)
	idx = utils.WriteBytes(buffer, idx, keyDataPgMessage.Pack())

	readyForQueryPgMessage := protocol.BuildReadyForQueryPgMessage(byte('I'))
	idx = utils.WriteBytes(buffer, idx, readyForQueryPgMessage.Pack())

	return buffer[:idx]
}

func queryResponsePackShim() []byte {
	buffer := make([]byte, 1024)
	rowDescriptionPgMessage := protocol.BuildRowDescriptionPgMessage(
		map[string][]int{
			"?column?": {0, 0, 0x17, 4, -1, 0},
		},
	)
	dataRowPgMessage := protocol.BuildDataRowPgMessage([][]byte{{0x31}})
	commandCompletePgMessage := protocol.BuildCommandCompletePgMessage("SELECT 1")
	readyForQueryPgMessage := protocol.BuildReadyForQueryPgMessage(byte('I'))

	idx := utils.WriteBytes(buffer, 0, rowDescriptionPgMessage.Pack())
	idx = utils.WriteBytes(buffer, idx, dataRowPgMessage.Pack())
	idx = utils.WriteBytes(buffer, idx, commandCompletePgMessage.Pack())
	idx = utils.WriteBytes(buffer, idx, readyForQueryPgMessage.Pack())

	return buffer[:idx]
}

func handleQuery(query string, client *ClientConnection) {
	cluster := &client.Ctx.Database.Clusters[0]
	server := server.CreateServerConnection(cluster)
	server.IssueQuery(query)

	// Copy messages directly from server to client
	// until we get a ready for query message
	for {
		rm, err := protocol.GetRawPgMessage(server.Conn)
		if err != nil {
			slog.Error("Error While handling Query", "error", err)
		}
		switch rm.Kind {
		case protocol.BMESSAGE_READY_FOR_QUERY:
			client.Write(rm.Pack())
			return
		default:
			client.Write(rm.Pack())
		}
	}
}

func ConnectionLoop(conn net.Conn, database *config.DatabaseConfig) {
	defer conn.Close()
	raw_message, err := protocol.GetRawStartupPgMessage(conn)
	if err != nil {
		slog.Error("Error: ", err)
		return
	}
	startPgMessage := &protocol.StartupPgMessage{}
	startPgMessage, err = startPgMessage.Unpack(raw_message)
	if err != nil {
		slog.Error("Error: ", err)
		return
	}

	ctx := NewClientConnectionContext(startPgMessage, database)
	clientConnection := &ClientConnection{Conn: conn, Ctx: ctx}
	conn.Write(configPacketShim(ctx))

	for {
		raw_message, err := protocol.GetRawPgMessage(conn)
		if err != nil {
			slog.Error("Error: ", err)
			break
		}

		switch raw_message.Kind {
		case protocol.FMESSAGE_QUERY:
			queryPgMessage := &protocol.QueryPgMessage{}
			queryPgMessage, err := queryPgMessage.Unpack(raw_message)
			if err != nil {
				slog.Error("Error: ", err)
			}
			slog.Info("Recieved Query: ", "query", queryPgMessage.Query)
			handleQuery(queryPgMessage.Query, clientConnection)
		case protocol.FMESSAGE_TERMINATE:
			slog.Info("Terminating connection")
			return
		default:
			slog.Warn("Unknown message kind: ", "kind", fmt.Sprint(raw_message.Kind))
		}
	}
}
