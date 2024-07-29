package main

import (
	"crypto/md5"
	"encoding/hex"
	"log/slog"
	"net"
	"os"

	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/protocol/parsing"
)

// Functions for handling authentication with the server

// Calculate the md5 hash of the password, username, and salt
func getMd5Password(password string, username string, salt []byte) string {
	// Alocate enough space for the password, username, md5 hash, and salt
	firstPass := make([]byte, len(password)+len(username))
	secondPass := make([]byte, 32+len(salt))

	// Write password, and the username to the buffer
	n := copy(firstPass, []byte(password))
	copy(firstPass[n:], []byte(username))

	// Calculate the md5 hash of the password and username combination
	md5Bytes := md5.Sum(firstPass)

	// create a hex string from the md5 hash
	md5String := hex.EncodeToString(md5Bytes[:])

	// Write firstPass md5 hash, and the salt to the buffer
	n = copy(secondPass, md5String)
	copy(secondPass[n:], salt)

	// Calculate the md5 hash of the md5 hash and the salt
	md5Bytes = md5.Sum(secondPass)

	// Return the md5 hash as a string
	return "md5" + hex.EncodeToString(md5Bytes[:])
}

func handleServerAuth(
	conn net.Conn,
	clusterConfig *ClusterConfig,
	raw_message *protocol.RawPgMessage,
) {
	_, authIndicator := parsing.ParseInt32(raw_message.Data, 0)

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
