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
		"server_version":              "pgspanner-0.1",
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
	keyDataPgMessage := protocol.BuildBackendKeyDataPgMessage(ctx.ClientPid, ctx.ClientSecret)
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

func handleCancelRequest(
	cancelMessage *protocol.CancelRequestPgMessage,
	config *config.SpannerConfig,
	requester *server.ConnectionRequester,
) {
	slog.Info(
		"Recieved Cancel Request. Forwarding to server",
		"clientPid", cancelMessage.BackendPid,
		"clientSecret", cancelMessage.BackendKey,
	)
	// Get the server connection mapping
	serverProcesses, err := getSeverConnectionMapping(
		requester,
		// This is the client pid of the connection that sent the original query
		cancelMessage.BackendPid,
	)
	if err != nil {
		slog.Error("Error getting server connection mapping", "error", err)
		return
	}

	for _, serverProcess := range serverProcesses {
		database, ok := config.GetDatabaseConfigByName(serverProcess.DatabaseName)
		if !ok {
			slog.Error("Database config not found", "database", serverProcess.DatabaseName)
			continue
		}
		cluster, ok := database.GetClusterConfigByHostPort(serverProcess.ClusterHost, serverProcess.ClusterPort)
		if !ok {
			slog.Error("Cluster config not found", "cluster", serverProcess.ClusterHost)
			continue
		}
		serverConn, err := server.CreateUnititializedServerConnection(database, cluster)
		if serverProcess.BackendPid == serverConn.GetBackendPid() {
			continue
		}
		slog.Info(
			"Sending cancel request to server",
			"serverDatabase", serverProcess.DatabaseName,
			"serverCluster", fmt.Sprintf("%s:%d", serverProcess.ClusterHost, serverProcess.ClusterPort),
			"serverPid", serverProcess.BackendPid,
		)
		cancelRequest := protocol.BuildCancelRequestPgMessage(serverProcess.BackendPid, serverProcess.BackendKey)
		_, err = serverConn.Write(cancelRequest.Pack())
		if err != nil {
			slog.Error(
				"Error sending cancel request to server",
				"error", err,
				"serverDatabase", serverProcess.DatabaseName,
				"serverCluster", fmt.Sprintf("%s:%d", serverProcess.ClusterHost, serverProcess.ClusterPort),
				"serverPid", serverProcess.BackendPid,
			)
			return
		}
	}
}

func getServerConnection(
	requester *server.ConnectionRequester,
	database *config.DatabaseConfig,
	clusterHost string,
	clusterPort int,
	clientPid int,
) (*server.ServerConnection, error) {
	cluster, ok := database.GetClusterConfigByHostPort(clusterHost, clusterPort)
	if !ok {
		slog.Error("Specified cluster config does not exist")
		return nil, fmt.Errorf("Specified cluster config does not exist")
	}
	slog.Info(
		"Requesting Connection",
		"Cluster", cluster.Name,
		"Database", database.Name,
		"ClientPid", clientPid,
	)
	response := requester.RequestConnection(database.Name, cluster.Name, clientPid)

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

		return nil, errMsg
	}

	slog.Info(
		"Recieved Connection",
		"Cluster", cluster.Name,
		"Database", database.Name,
		"ConnectionPid", response.Conn.GetBackendPid(),
	)

	return response.Conn, nil
}

func getSeverConnectionMapping(
	requester *server.ConnectionRequester,
	clientPid int,
) ([]server.ServerProcessIdentity, error) {
	slog.Info(
		"Requesting Connection Mapping",
		"ClientPid", clientPid,
	)
	response := requester.RequestConnectionMapping(clientPid)

	if response.Result == server.RESULT_ERROR {
		slog.Error("Error Requesting Connection Mapping", "error", response.Detail.Error())
		return nil, response.Detail
	}

	return response.ConnMapping, nil
}

func handleQuery(
	query string,
	client *ClientConnection,
	requester *server.ConnectionRequester,
	database *config.DatabaseConfig,
) {
	// Get a connection from the pool
	cluster := database.Clusters[0]
	server, err := getServerConnection(requester, database, cluster.Host, cluster.Port, client.Ctx.ClientPid)
	if err != nil {
		if errMsg, ok := err.(*protocol.ErrorResponsePgMessage); ok {
			client.Write(buildErrorResponsePacket(errMsg))
			return
		} else {
			slog.Error("Error getting server connection", "error", err)
		}
		return
	}

	// ensure we return the connection to the pool
	defer requester.ReturnConnection(server, database.Name, database.Clusters[0].Name, client.Ctx.ClientPid)

	server.IssueQuery(query)

	for {
		rm, err := protocol.GetRawPgMessage(server.Conn)
		if err != nil {
			slog.Error("Error reading raw message in query handler", "error", err)
			return
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

func ConnectionLoop(conn net.Conn, config *config.SpannerConfig, connectionRequester *server.ConnectionRequester, clientPid int) {
	defer conn.Close()
	clientConnection := &ClientConnection{Conn: conn}

	rawMessage, err := protocol.GetRawStartupPgMessage(conn)
	if err != nil {
		slog.Error("Error getting raw startup message", "error", err)
		return
	}

	switch rawMessage.Kind {
	case protocol.FMESSAGE_CANCEL:
		if errMsg, ok := err.(*protocol.ErrorResponsePgMessage); ok {
			conn.Write(buildErrorResponsePacket(errMsg))
			return
		} else if err != nil {
			slog.Error("Error getting server connection", "error", err)
			return
		}
		cancelMessage := &protocol.CancelRequestPgMessage{}
		cancelMessage, err = cancelMessage.Unpack(rawMessage)
		handleCancelRequest(
			cancelMessage,
			config,
			connectionRequester,
		)
		return
	case protocol.FMESSAGE_STARTUP:
		break
	}

	startPgMessage := &protocol.StartupPgMessage{}
	startPgMessage, err = startPgMessage.Unpack(rawMessage)
	if err != nil {
		slog.Error("Error Unpacking startup message", "error", err)
		return
	}
	database, _ := config.GetDatabaseConfigByName(startPgMessage.Database)

	ctx := NewClientConnectionContext(startPgMessage, database, clientPid)
	clientConnection.Ctx = ctx
	conn.Write(configPacketShim(ctx))

	for {
		rawMessage, err := protocol.GetRawPgMessage(conn)
		if err != nil {
			slog.Error("Error: ", err)
			break
		}

		switch rawMessage.Kind {
		case protocol.FMESSAGE_QUERY:
			queryPgMessage := &protocol.QueryPgMessage{}
			queryPgMessage, err := queryPgMessage.Unpack(rawMessage)
			if err != nil {
				slog.Error("Error: ", err)
			}
			slog.Info("Recieved Query: ", "query", queryPgMessage.Query)
			handleQuery(queryPgMessage.Query, clientConnection, connectionRequester, database)
		case protocol.FMESSAGE_CANCEL:
			slog.Info("Recieved Cancel Request with no query running. Ignoring")
		case protocol.FMESSAGE_TERMINATE:
			slog.Info("Terminating client connection")
			return
		case protocol.BMESSAGE_ERROR_RESPONSE:
			errorPgMessage := &protocol.ErrorResponsePgMessage{}
			errorPgMessage, err := errorPgMessage.Unpack(rawMessage)
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
			clientConnection.Write(rawMessage.Pack())
		default:
			slog.Warn("Unknown message kind: ", "kind", fmt.Sprint(rawMessage.Kind))
		}
	}
}
