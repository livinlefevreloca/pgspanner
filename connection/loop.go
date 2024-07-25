package connection

import (
	"fmt"
	"net"

	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/utils"
)

func staticServerConfiguration(ctx *ConnectionContext) *map[string]string {
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
func configPacketShim(ctx *ConnectionContext) []byte {
	buffer := make([]byte, 1024)
	idx := 0
	serverConfig := staticServerConfiguration(ctx)

	// For now just send an authentication ok message
	authMessage := protocol.BuildAuthenticationOkMessage()
	idx = utils.WriteBytes(buffer, idx, authMessage.Pack())
	for k, v := range *serverConfig {
		configMessage := protocol.BuildParameterStatusMessage(k, v)
		idx = utils.WriteBytes(buffer, idx, configMessage.Pack())
	}
	keyDataMessage := protocol.BuildBackendKeyDataMessage(1337, 1)
	idx = utils.WriteBytes(buffer, idx, keyDataMessage.Pack())

	readyForQueryMessage := protocol.BuildReadyForQueryMessage(byte('I'))
	idx = utils.WriteBytes(buffer, idx, readyForQueryMessage.Pack())

	return buffer[:idx]
}

func queryResponsePackShim() []byte {
	buffer := make([]byte, 1024)
	rowDescriptionMessage := protocol.BuildRowDescriptionMessage(
		map[string][]int{
			"?column?": {0, 0, 0x17, 4, -1, 0},
		},
	)
	dataRowMessage := protocol.BuildDataRowMessage([][]byte{{0x31}})
	commandCompleteMessage := protocol.BuildCommandCompleteMessage("SELECT 1")
	readyForQueryMessage := protocol.BuildReadyForQueryMessage(byte('I'))

	idx := utils.WriteBytes(buffer, 0, rowDescriptionMessage.Pack())
	idx = utils.WriteBytes(buffer, idx, dataRowMessage.Pack())
	idx = utils.WriteBytes(buffer, idx, commandCompleteMessage.Pack())
	idx = utils.WriteBytes(buffer, idx, readyForQueryMessage.Pack())

	return buffer[:idx]
}

func ConnectionLoop(conn net.Conn) {
	raw_message, err := protocol.GetRawStartupMessage(conn)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	startMessage := &protocol.StartupMessage{}
	startMessage, err = startMessage.Unpack(raw_message)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	ctx := NewConnectionContext(startMessage)
	conn.Write(configPacketShim(ctx))

	for {
		raw_message, err := protocol.GetRawMessage(conn)
		if err != nil {
			fmt.Println("Error: ", err)
			break
		}

		switch raw_message.Kind {
		case protocol.FMESSAGE_QUERY:
			queryMessage := &protocol.QueryMessage{}
			queryMessage, err := queryMessage.Unpack(raw_message)
			if err != nil {
				fmt.Println("Error: ", err)
			}
			fmt.Println("Query: ", queryMessage.Query)

			conn.Write(queryResponsePackShim())
		default:
			fmt.Println("Unknown message kind: ", raw_message.Kind)
		}
	}

}
