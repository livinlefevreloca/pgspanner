package main

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/protocol/parsing"
)

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
	idx = parsing.WriteBytes(buffer, idx, authPgMessage.Pack())
	for k, v := range *serverConfig {
		configPgMessage := protocol.BuildParameterStatusPgMessage(k, v)
		idx = parsing.WriteBytes(buffer, idx, configPgMessage.Pack())
	}
	keyDataPgMessage := protocol.BuildBackendKeyDataPgMessage(ctx.ClientPid, ctx.ClientSecret)
	idx = parsing.WriteBytes(buffer, idx, keyDataPgMessage.Pack())

	readyForQueryPgMessage := protocol.BuildReadyForQueryPgMessage(byte('I'))
	idx = parsing.WriteBytes(buffer, idx, readyForQueryPgMessage.Pack())

	return buffer[:idx]
}

func buildErrorResponsePacket(errMsg *protocol.ErrorResponsePgMessage) []byte {
	packet := errMsg.Pack()
	packet = append(packet, protocol.BuildReadyForQueryPgMessage(byte('I')).Pack()...)
	return packet
}

func handleCancelRequest(
	cancelMessage *protocol.CancelRequestPgMessage,
	config *SpannerConfig,
	requester *ConnectionRequester,
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
		cluster, ok := database.GetClusterConfigByHostPort(serverProcess.GetAddr())
		if !ok {
			slog.Error("Cluster config not found", "cluster", serverProcess.ClusterHost)
			continue
		}
		serverConn, err := CreateUnititializedServerConnection(database, cluster)
		defer serverConn.Close()
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
	requester *ConnectionRequester,
	database *DatabaseConfig,
	clusterAddr string,
	clientPid int,
) (*ServerConnection, error) {
	slog.Info(
		"Requesting Connection",
		"Cluster", clusterAddr,
		"Database", database.Name,
		"ClientPid", clientPid,
	)
	response := requester.RequestConnection(database.Name, clusterAddr, clientPid)

	switch response.Result {
	case RESULT_SUCCESS:
		break
	case RESULT_ERROR:
		slog.Error("Error Requesting Connection", "error", response.Detail.Error())
		params := map[string]string{
			protocol.NOTICE_KIND_SEVERITY_NONLOCALIZED: "ERROR",
			protocol.NOTICE_KIND_SEVERITY_LOCALIZED:    "ERROR",
			protocol.NOTICE_KIND_CODE:                  "08000",
			protocol.NOTICE_KIND_MESSAGE:               fmt.Sprintf("Failed to open connection to cluster %s for database %s", clusterAddr, database.Name),
			protocol.NOTICE_KIND_DETAIL:                response.Detail.Error(),
		}
		errMsg := protocol.BuildErrorResponsePgMessage(params)

		return nil, errMsg
	}

	slog.Info(
		"Recieved Connection",
		"Cluster", clusterAddr,
		"Database", database.Name,
		"ConnectionPid", response.Conn.GetBackendPid(),
	)

	return response.Conn, nil
}

func getSeverConnectionMapping(
	requester *ConnectionRequester,
	clientPid int,
) ([]ServerProcessIdentity, error) {
	slog.Info(
		"Requesting Connection Mapping",
		"ClientPid", clientPid,
	)
	response := requester.RequestConnectionMapping(clientPid)

	if response.Result == RESULT_ERROR {
		slog.Error("Error Requesting Connection Mapping", "error", response.Detail.Error())
		return nil, response.Detail
	}

	return response.ConnMapping, nil
}

func handleQuery(
	query string,
	client *ClientConnection,
	requester *ConnectionRequester,
	database *DatabaseConfig,
) {
	// Get a connection from the pool
	cluster := database.Clusters[0]
	server, err := getServerConnection(requester, database, cluster.GetAddr(), client.Ctx.ClientPid)
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
	defer requester.ReturnConnection(server, database.Name, cluster.GetAddr(), client.Ctx.ClientPid)

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

func ConnectionLoop(conn net.Conn, config *SpannerConfig, connectionRequester *ConnectionRequester, clientPid int) {
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
	database, ok := config.GetDatabaseConfigByName(startPgMessage.Database)
	if !ok {
		slog.Error("Database not found", "database", startPgMessage.Database)
		errMsg := protocol.BuildErrorResponsePgMessage(map[string]string{
			protocol.NOTICE_KIND_SEVERITY_NONLOCALIZED: "FATAL",
			protocol.NOTICE_KIND_SEVERITY_LOCALIZED:    "FATAL",
			protocol.NOTICE_KIND_CODE:                  "08000",
			protocol.NOTICE_KIND_MESSAGE:               fmt.Sprintf("Database %s not found", startPgMessage.Database),
			protocol.NOTICE_KIND_DETAIL:                "",
		})
		conn.Write(errMsg.Pack())
		return
	}

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
			slog.Info("Terminating client connection", "clientPid", clientPid)
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

func clientConnectionHandler(
	config *SpannerConfig,
	keepAlive *KeepAlive,
	connectionReqester *ConnectionRequester,
) {
	// Start the client pid counter
	clientPid := 0
	slog.Info("Client connection handler started")
	if config.ListenAddr == "localhost" || config.ListenAddr == "" {
		config.ListenAddr = "127.0.0.1"
	}
	addr := net.TCPAddr{
		IP:   net.ParseIP(config.ListenAddr),
		Port: config.ListenPort,
		Zone: "",
	}

	l, err := net.ListenTCP("tcp", &addr)
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Listening on port 8000")
	defer l.Close()
	for {
		l.SetDeadline(time.Now().Add(TIMEOUT))
		conn, err := l.Accept()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			keepAlive.Notify()
			slog.Debug("Client connection handler loop timeout")
			continue
		} else if err != nil {
			log.Fatal(err)
			return
		}
		slog.Info("Client connected. Starting connection loop...")
		go ConnectionLoop(conn, config, connectionReqester, clientPid)
		clientPid++
		keepAlive.Notify()
		slog.Debug("Client connection handler loop")
	}
}
