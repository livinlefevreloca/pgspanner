package server

import (
	"crypto/md5"
	"encoding/hex"
	"log/slog"
	"net"
	"os"

	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/utils"
)

// Functions for handling authentication with the server

// Calculate the md5 hash of the password, username, and salt
func getMd5Password(password string, username string, salt []byte) string {
	// Alocate enough space for the password, username, md5 hash, and salt
	firstPass := make([]byte, 0, len(password)+len(username))
	secondPass := make([]byte, 0, 32+len(salt))
	// Write password, and the username to the buffer
	firstPass = append(firstPass, []byte(password)...)
	firstPass = append(firstPass, []byte(username)...)
	// Calculate the md5 hash of the password and username combination
	md5Bytes := md5.Sum(firstPass)
	// create a hex string from the md5 hash
	md5String := hex.EncodeToString(md5Bytes[:])
	// Write firstPass md5 hash, and the salt to the buffer
	secondPass = append(secondPass, md5String...)
	secondPass = append(secondPass, salt...)
	// Calculate the md5 hash of the md5 hash and the salt
	md5Bytes = md5.Sum(secondPass)

	// Return the md5 hash as a string
	return "md5" + hex.EncodeToString(md5Bytes[:])
}

func handleServerAuth(
	conn net.Conn,
	clusterConfig *config.ClusterConfig,
	raw_message *protocol.RawPgMessage,
) {
	_, authIndicator := utils.ParseInt32(raw_message.Data, 0)

	switch authIndicator {
	case protocol.AUTH_OK:
		return
	case protocol.AUTH_MD5_PASSWORD:
		md5Message := &protocol.AuthenticationMD5PasswordPgMessage{}
		md5Message, err := md5Message.Unpack(raw_message)
		if err != nil {
			panic(err)
		}

		password := os.Getenv(clusterConfig.PasswordEnv)
		md5Password := getMd5Password(password, clusterConfig.User, md5Message.Salt)
		md5PasswordMessage := protocol.BuildPasswordMessage(md5Password)
		conn.Write(md5PasswordMessage.Pack())
	default:
		slog.Error("Unknown authentication type", "indicator", authIndicator)
	}
}

type serverConnectionContext struct {
	Parmeters  map[string]string
	BackendPid int
	BackendKey int
	Cluster    *config.ClusterConfig
}

func newServerConnectionContext(clusterConfig *config.ClusterConfig) *serverConnectionContext {
	return &serverConnectionContext{
		Parmeters: make(map[string]string),
		Cluster:   clusterConfig,
	}
}

func (s *serverConnectionContext) GetParameter(key string) string {
	return s.Parmeters[key]
}

func (s *serverConnectionContext) SetParameter(key string, value string) {
	s.Parmeters[key] = value
}

func handleStartup(
	conn net.Conn,
	clusterConfig *config.ClusterConfig,
	serverContext *serverConnectionContext,
) *serverConnectionContext {
	for {
		raw_message, err := protocol.GetRawPgMessage(conn)
		if err != nil {
			panic(err)
		}

		switch raw_message.Kind {
		case protocol.BMESSAGE_AUTH:
			slog.Info("Received authentication message")
			handleServerAuth(conn, clusterConfig, raw_message)
		case protocol.BMESSAGE_PARAMETER_STATUS:
			parameterMessage := &protocol.ParameterStatusPgMessage{}
			parameterMessage, err = parameterMessage.Unpack(raw_message)
			if err != nil {
				panic(err)
			}
			serverContext.SetParameter(parameterMessage.Name, parameterMessage.Value)
		case protocol.BMESSAGE_BACKEND_KEY_DATA:
			backendKeyData := &protocol.BackendKeyDataPgMessage{}
			backendKeyData, err = backendKeyData.Unpack(raw_message)
			if err != nil {
				panic(err)
			}
			serverContext.BackendPid = backendKeyData.Pid
			serverContext.BackendKey = backendKeyData.SecretKey
		case protocol.BMESSAGE_READY_FOR_QUERY:
			slog.Info("Servcer Connection established")
			return serverContext
		default:
			slog.Error("Unknown message type", "kind", raw_message.Kind)
			return nil
		}
	}

}

// An object representing a connection to a server
type ServerConnection struct {
	Conn    net.Conn
	Context *serverConnectionContext
}

// Implement the Writer interface for the ServerConnection
func (s *ServerConnection) Write(p []byte) (n int, err error) {
	if n, err := s.Conn.Write(p); err != nil {
		slog.Error("Error writing to server", "error", err)
		return n, err
	} else {
		return n, nil
	}
}

// implement the Reader interface for the ServerConnection
func (s *ServerConnection) Read(p []byte) (n int, err error) {
	if n, err := s.Conn.Read(p); err != nil {
		slog.Error("Error reading from server", "error", err)
		return n, err
	} else {
		return n, nil
	}
}

func CreateServerConnection(clusterConfig *config.ClusterConfig) *ServerConnection {
	addrs, err := net.LookupHost(clusterConfig.Host)
	if err != nil {
		panic(err)
	}

	hostAddr := net.TCPAddr{
		IP:   net.ParseIP(addrs[0]),
		Port: clusterConfig.Port,
	}

	conn, err := net.DialTCP("tcp", nil, &hostAddr)
	if err != nil {
		panic(err)
	}

	serverContext := newServerConnectionContext(clusterConfig)
	startupMessage := protocol.BuildStartupMessage(clusterConfig)

	conn.Write(startupMessage.Pack())

	serverContext = handleStartup(conn, clusterConfig, serverContext)

	serverConnection := ServerConnection{
		Conn:    conn,
		Context: serverContext,
	}

	return &serverConnection
}

func (s *ServerConnection) IssueQuery(query string) {
	queryMessage := protocol.BuildQueryMessage(query)
	s.Conn.Write(queryMessage.Pack())
}
