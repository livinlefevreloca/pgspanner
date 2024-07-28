package server

import (
	"log/slog"
	"net"
	"time"

	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/protocol"
)

// An object representing a connection to a server
type ServerConnection struct {
	Conn       net.Conn
	Context    *serverConnectionContext
	createTime int64
	poisoned   bool
}

func (s *ServerConnection) IsPoisoned() bool {
	return s.poisoned
}

func (s *ServerConnection) GetBackendPid() int {
	return s.Context.ServerIdentity.BackendPid
}

func (s *ServerConnection) GetBackendKey() int {
	return s.Context.ServerIdentity.BackendKey
}

func (s *ServerConnection) GetServerIdentity() ServerProcessIdentity {
	return s.Context.ServerIdentity
}

func (s *ServerConnection) GetClusterConfig() *config.ClusterConfig {
	return s.Context.Cluster
}

func (s *ServerConnection) GetDatabaseConfig() *config.DatabaseConfig {
	return s.Context.Database
}

// Implement the Writer interface for the ServerConnection
func (s *ServerConnection) Write(p []byte) (n int, err error) {
	if n, err := s.Conn.Write(p); err != nil {
		slog.Error("Error writing to server", "error", err)
		s.poisoned = true
		return n, err
	} else {
		return n, nil
	}
}

// implement the Reader interface for the ServerConnection
func (s *ServerConnection) Read(p []byte) (n int, err error) {
	if n, err := s.Conn.Read(p); err != nil {
		slog.Error("Error reading from server", "error", err)
		s.poisoned = true
		return n, err
	} else {
		return n, nil
	}
}

func (s *ServerConnection) GetAge() int64 {
	return time.Now().Unix() - s.createTime
}

func (s *ServerConnection) Close() {
	s.Conn.Close()
}

func handleStartup(
	server *ServerConnection,
) *ServerConnection {
	for {
		raw_message, err := protocol.GetRawPgMessage(server.Conn)
		if err != nil {
			panic(err)
		}
		clusterConfig := server.GetClusterConfig()
		databaseConfig := server.Context.Database

		switch raw_message.Kind {
		case protocol.BMESSAGE_AUTH:
			handleServerAuth(server.Conn, clusterConfig, raw_message)
		case protocol.BMESSAGE_PARAMETER_STATUS:
			parameterMessage := &protocol.ParameterStatusPgMessage{}
			parameterMessage, err = parameterMessage.Unpack(raw_message)
			if err != nil {
				panic(err)
			}
			server.Context.SetParameter(parameterMessage.Name, parameterMessage.Value)
		case protocol.BMESSAGE_BACKEND_KEY_DATA:
			backendKeyData := &protocol.BackendKeyDataPgMessage{}
			backendKeyData, err = backendKeyData.Unpack(raw_message)
			if err != nil {
				panic(err)
			}
			serverIdentity := ServerProcessIdentity{
				BackendPid:   backendKeyData.Pid,
				BackendKey:   backendKeyData.SecretKey,
				ClusterHost:  clusterConfig.Host,
				DatabaseName: databaseConfig.Name,
				ClusterPort:  clusterConfig.Port,
			}
			server.Context.ServerIdentity = serverIdentity
		case protocol.BMESSAGE_READY_FOR_QUERY:
			return server
		default:
			slog.Error("Unknown message type", "kind", raw_message.Kind)
			return nil
		}
	}

}

func CreateUnititializedServerConnection(
	databaseConfig *config.DatabaseConfig,
	clusterConfig *config.ClusterConfig,
) (*ServerConnection, error) {
	serverContext := newServerConnectionContext(clusterConfig, databaseConfig)
	addrs, err := net.LookupHost(clusterConfig.Host)
	if err != nil {
		return nil, err
	}

	hostAddr := net.TCPAddr{
		IP:   net.ParseIP(addrs[0]),
		Port: clusterConfig.Port,
	}

	conn, err := net.DialTCP("tcp", nil, &hostAddr)
	if err != nil {
		return nil, err
	}
	serverConnection := ServerConnection{
		Conn:       conn,
		Context:    serverContext,
		createTime: time.Now().Unix(),
	}

	return &serverConnection, nil
}

func CreateServerConnection(
	databaseConfig *config.DatabaseConfig,
	clusterConfig *config.ClusterConfig,
) (*ServerConnection, error) {

	server, err := CreateUnititializedServerConnection(databaseConfig, clusterConfig)
	if err != nil {
		return nil, err
	}

	startupMessage := protocol.BuildStartupMessage(clusterConfig)
	server.Write(startupMessage.Pack())

	server = handleStartup(server)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *ServerConnection) IssueQuery(query string) {
	queryMessage := protocol.BuildQueryMessage(query)
	s.Conn.Write(queryMessage.Pack())
}
