package main

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/protocol/parsing"
)

// Functions for handling authentication with the server

func saslPrep(input string) string {
	return input
}

func getSupportedSASLMechanisms() []string {
	return []string{"SCRAM-SHA-256"}
}

func generateNonce(length int) string {
	nonce := make([]byte, length)
	// golang stdlib rand.Read function reads random bytes from a global
	// Reader which is a shared instance of a cryptographically secure
	// random number generator. More details can be found at
	// https://pkg.go.dev/crypto/rand#Read
	rand.Read(nonce)
	// Base64 encode the nonce to make it ASCII
	return base64.StdEncoding.EncodeToString(nonce)
}

func parseSASLData(data string) map[string]string {
	saslData := make(map[string]string)
	parts := strings.Split(data, ",")
	for _, part := range parts {
		keyValue := strings.Split(part, "=")
		saslData[keyValue[0]] = keyValue[1]
	}
	return saslData
}

func scramSaltedPassword(password string, salt []byte, iterations int) []byte {
	hmac := sha256.New()
	hmac.Write([]byte(password))
	hmac.Write(salt)
	// big endian encoding of 1u32
	unInitPrev := hmac.Sum([]byte{1, 0, 0, 0})
	result := unInitPrev

	// iterate for the remaining iterations - 1 times
	for i := 2; i <= iterations; i++ {
		hmac.Reset()
		unInitPrev = hmac.Sum(unInitPrev)
		for j := 0; j < len(result); j++ {
			result[j] ^= unInitPrev[j]
		}
	}
	return result
}

func scramClientKey(saltedPassword []byte) []byte {
	hmac := sha256.New()
	hmac.Write(saltedPassword)
	return hmac.Sum([]byte("Client Key"))
}

func buildServerFirstMessage(
	salt []byte,
	iterations int,
	clientNonce string,
	serverNonce string,
) string {
	// Base64 encode the salt
	saltBase64 := base64.StdEncoding.EncodeToString(salt)
	// Build the server first message
	serverFirstMessage := "r=" + clientNonce + serverNonce
	serverFirstMessage += ",s=" + saltBase64
	serverFirstMessage += ",i=" + strconv.Itoa(iterations)

	return serverFirstMessage
}

func verifyServerSignature(serverSignature string) bool {
	// TODO: Implement server signature verification
	return true
}

// This is the meat of the SCRAM-SHA-256 authentication. I wont
// pretend to fully understand the why of all of what is going on here but
// I will lay out the steps as I understand them.
func calculateClientProof(
	password string,
	salt []byte,
	iterations int,
	clientNonce string,
	serverNonce string,
	messageWithoutProof []byte,
) string {
	// Salt the password `iterations` times (usually 4096)
	saltedPassword := scramSaltedPassword(password, salt, iterations)
	// Calculate the client key from the salted password
	clientKey := scramClientKey(saltedPassword)
	// Calculate the stored key from the client key
	storedKey := sha256.Sum256(clientKey)
	hmac := sha256.New()
	// Start a new HMAC with the stored key
	hmac.Write(storedKey[:])
	// Write the original message with channel binding info
	hmac.Write([]byte("n=,r=" + clientNonce))
	// Write the server first message
	hmac.Write([]byte(buildServerFirstMessage(salt, iterations, clientNonce, serverNonce)))
	// Write the message without the proof and calculate the client signature
	clientSignature := hmac.Sum(messageWithoutProof)

	// XOR the client signature with the client key
	for i := 0; i < len(clientSignature); i++ {
		clientSignature[i] ^= clientKey[i]
	}

	// Return the base64 encoded client signature
	return base64.StdEncoding.EncodeToString(clientSignature)

}

func handleSASLIntitialRequest(
	conn net.Conn,
	rawMessage *protocol.RawPgMessage,
) (string, error) {
	authSASLRequest := &protocol.AuthenticationSASLPgMessage{}
	authSASLRequest, err := authSASLRequest.Unpack(rawMessage)
	if err != nil {
		return "", err
	}

	idx := 0
	// Allocate a buffer for the response. 256 bytes will be more than enough
	// for the initial response which only contains the client nonce which is
	// 18 bytes long
	responseData := make([]byte, 256)

	// We only currently support the SCRAM-SHA-256 SASL mechanism
	supportedMechanisms := getSupportedSASLMechanisms()
	var chosenMechanism string
	for _, mechanism := range authSASLRequest.AuthMechanisms {
		if slices.Contains(supportedMechanisms, mechanism) {
			chosenMechanism = mechanism
			break
		}
	}
	if chosenMechanism == "" {
		return "", errors.New("No supported SASL mechanism")
	}

	// formulate the scram-sha-256 initial response
	//
	// We do not support channel binding
	idx = parsing.WriteByte(responseData, idx, 'n')

	// Usernam is left blank since the sever will use the username
	// from the intitial startup message. This avoids the need to
	// run SASLprep on the username
	idx = parsing.WriteBytes(responseData, idx, []byte(",,n="))

	// Write the client nonce
	clientNonce := generateNonce(18)
	idx = parsing.WriteBytes(responseData, idx, []byte(",r="+clientNonce))

	saslInitialResponse := protocol.BuildSASLInitialResponseMessage(chosenMechanism, responseData)
	conn.Write(saslInitialResponse.Pack())

	return clientNonce, nil
}

func handleSASLContinue(
	conn net.Conn,
	rawMessage *protocol.RawPgMessage,
	clientNonce string,
	password string,
) error {
	authSASLContinue := &protocol.AuthenticationSASLContinuePgMessage{}
	authSASLContinue, err := authSASLContinue.Unpack(rawMessage)
	if err != nil {
		slog.Error("Error unpacking SASL continue message")
		return err
	}

	saslData := string(authSASLContinue.Data)
	saslDataMap := parseSASLData(saslData)

	// Verify the server nonce
	fullNonce := saslDataMap["r"]
	if !strings.HasPrefix(fullNonce, clientNonce) {
		slog.Error("Failed SASL auth, server nonce does not match client nonce")
		return errors.New("Server nonce does not match client nonce")
	}
	serverNonce := fullNonce[len(clientNonce):]

	serverSalt := saslDataMap["s"]
	decodedSalt, err := base64.StdEncoding.DecodeString(serverSalt)
	if err != nil {
		slog.Error("Failed to decode server salt")
		return err
	}
	serverIterations, err := strconv.Atoi(saslDataMap["i"])
	if err != nil {
		slog.Error("Failed to parse server iterations")
		return err
	}

	// 256 bytes should be more than enough for the response
	// but we will use WriteBytesSafe to ensure the buffer i
	// expanded if needed
	saslResponseBuffer := make([]byte, 256)

	idx := 0
	// "c=biws" -> base64("n,,n=")ntNonce
	// Write initial data plus the server nonce
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte("c=biws,r="+serverNonce))

	saslPreppedPassword := saslPrep(password)
	// Calculate the client proof
	clientProof := calculateClientProof(
		saslPreppedPassword,
		decodedSalt,
		serverIterations,
		clientNonce,
		serverNonce,
		saslResponseBuffer,
	)
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte(",p="))
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte(clientProof))

	authSASLResponse := protocol.BuildSASLResponseMessage(saslResponseBuffer)
	conn.Write(authSASLResponse.Pack())

	return nil
}

func handleSASLFinal(conn net.Conn, rawMessage *protocol.RawPgMessage) string {
	// TODO: Implement handling of the SASL final message
	return ""
}

func handleSASLAuth(
	conn net.Conn,
	clusterConfig *ClusterConfig,
	rawMessage *protocol.RawPgMessage,
) error {

	currentState := "BuildIntialResponse"
	// Save the client nonce to verify the server's response
	clientNonce, err := handleSASLIntitialRequest(conn, rawMessage)
	if err != nil {
		slog.Error("Error handling SASL initial request", "state", currentState)
		return err
	}
	currentState = "ReadSaslContinue"

	for {
		rawMessage, err = protocol.GetRawPgMessage(conn)
		if err != nil {
			slog.Error("Error SASL message from server", "state", currentState)
		}

		var authIndicator int
		switch rawMessage.Kind {
		case protocol.BMESSAGE_AUTH:
			_, authIndicator = parsing.ParseInt32(rawMessage.Data, 0)
			if err != nil {
				slog.Error("Error reading SASL authentication indicator", "state", currentState)
				return err
			}
			switch authIndicator {
			case protocol.AUTH_SASL_CONTINUE:
				currentState = "BuildSASLResponse"
				password := os.Getenv(clusterConfig.PasswordEnv)
				err = handleSASLContinue(conn, rawMessage, clientNonce, password)
				if err != nil {
					slog.Error("Error calculating client Response to challenge", "state", currentState)
					return err
				}
				currentState = "ReadSaslFinal"
				// back to the top of the loop
			case protocol.AUTH_SASL_FINAL:
				currentState = "BuildSASLFinal"
				serverSignature := handleSASLFinal(conn, rawMessage)
				currentState = "VerifyServerSignature"
				if !verifyServerSignature(serverSignature) {
					slog.Error("Server signature verification failed", "state", currentState)
					return errors.New("Server signature verification failed")
				}
				currentState = "SASLAuthComplete"
				slog.Info("SASL authentication complete", "state", currentState)
				return nil
			default:
				slog.Error(
					"Unknown SASL authentication indicator",
					"indicator", authIndicator,
					"state", currentState,
				)
				return errors.New("Unknown SASL authentication indicator")
			}
		case protocol.BMESSAGE_ERROR_RESPONSE:
			slog.Error("Error response from server", "state", currentState)
			break
		default:
			slog.Error(
				"Unexpected message type",
				"kind", rawMessage.Kind,
				"state", currentState,
			)
			return errors.New("Unexpected message type")
		}

	}

}

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

func handleMD5Auth(conn net.Conn, clusterConfig *ClusterConfig, rawMessage *protocol.RawPgMessage) {
	md5Message := &protocol.AuthenticationMD5PasswordPgMessage{}
	md5Message, err := md5Message.Unpack(rawMessage)
	if err != nil {
		panic(err)
	}

	password := os.Getenv(clusterConfig.PasswordEnv)
	md5Password := getMd5Password(password, clusterConfig.User, md5Message.Salt)
	md5PasswordMessage := protocol.BuildPasswordMessage(md5Password)
	conn.Write(md5PasswordMessage.Pack())
}

func handleServerAuth(
	conn net.Conn,
	clusterConfig *ClusterConfig,
	rawMessage *protocol.RawPgMessage,
) {
	_, authIndicator := parsing.ParseInt32(rawMessage.Data, 0)

	switch authIndicator {
	case protocol.AUTH_OK:
		return // No authentication required
	case protocol.AUTH_MD5_PASSWORD:
		handleMD5Auth(conn, clusterConfig, rawMessage)
	case protocol.AUTH_SASL:
		handleSASLAuth(conn, clusterConfig, rawMessage)
	default:
		slog.Error("Unknown authentication type", "indicator", authIndicator)
	}
}
