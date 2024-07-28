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

func buildErrorResponsePacket(errMsg *protocol.ErrorResponsePgMessage) []byte {
	packet := errMsg.Pack()
	packet = append(packet, protocol.BuildReadyForQueryPgMessage(byte('I')).Pack()...)
	return packet
}

func handleQuery(
	query string,
	client *ClientConnection,
	requester *server.ConnectionRequester,
	database *config.DatabaseConfig,
) {
	// For now just read from the first cluster
	cluster := database.Clusters[0]
	slog.Info(
		"Requesting Connection",
		"Cluster", cluster.Name,
		"Database", database.Name,
	)
	response := requester.RequestConnection(database.Name, cluster.Name)
	switch response.Result {
	case server.RESULT_SUCCESS:
		break
	case server.RESULT_ERROR:
		slog.Error("Error Requesting Connection", "error", response.Detail.Error())
		params := map[string]string{
			protocol.NOTICE_KIND_SEVERITY_NONLOCALIZED: "ERROR",
			protocol.NOTICE_KIND_SEVERITY_LOCALIZED:    "ERROR",
			protocol.NOTICE_KIND_CODE:                  "08000",
			protocol.NOTICE_KIND_MESSAGE:               fmt.Sprintf("Failed to open connection to cluster %s for database %s", cluster.Name, database.Name),
			protocol.NOTICE_KIND_DETAIL:                response.Detail.Error(),
		}
		errMsg := protocol.BuildErrorResponsePgMessage(params)
		client.Write(buildErrorResponsePacket(errMsg))

		return
	}

	slog.Info(
		"Recieved Connection",
		"Cluster", cluster.Name,
		"Database", database.Name,
		"ConnectionPid", response.Conn.GetBackendPid(),
	)
	server := response.Conn
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
			requester.ReturnConnection(server, database.Name, cluster.Name)
			return
		default:
			client.Write(rm.Pack())
		}
	}
}

func ConnectionLoop(conn net.Conn, config *config.SpannerConfig, connectionRequester *server.ConnectionRequester) {
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
	database, _ := config.GetDatabaseConfigByName(startPgMessage.Database)

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
			handleQuery(queryPgMessage.Query, clientConnection, connectionRequester, database)
		case protocol.FMESSAGE_TERMINATE:
			slog.Info("Terminating client connection")
			return
		case protocol.BMESSAGE_ERROR_RESPONSE:
			errorPgMessage := &protocol.ErrorResponsePgMessage{}
			errorPgMessage, err := errorPgMessage.Unpack(raw_message)
			if err != nil {
				slog.Error("Error parsing error response from server", "error", err)
			}
			slog.Error(
				"Error from server",
				"Severity", errorPgMessage.GetErrorResponseField(protocol.NOTICE_KIND_SEVERITY_NONLOCALIZED),
				"Code", errorPgMessage.GetErrorResponseField(protocol.NOTICE_KIND_CODE),
				"Message", errorPgMessage.GetErrorResponseField(protocol.NOTICE_KIND_MESSAGE),
				"Detail", errorPgMessage.GetErrorResponseField(protocol.NOTICE_KIND_DETAIL),
			)
			clientConnection.Write(raw_message.Pack())
		default:
			slog.Warn("Unknown message kind: ", "kind", fmt.Sprint(raw_message.Kind))
		}
	}
}
