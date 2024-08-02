package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"

	"github.com/livinlefevreloca/pgspanner/protocol"
	"github.com/livinlefevreloca/pgspanner/protocol/parsing"
)

var b64 = base64.StdEncoding

const (
	SHA256_BLOCK_SIZE = 32
)

func mapContainsKey[K comparable, V any](m map[K]V, key K) bool {
	_, ok := m[key]
	return ok
}

// Functions for handling authentication with the server
func saslPrep(input string) []byte {
	return []byte(input)
}

func getSupportedSASLMechanisms() []string {
	return []string{"SCRAM-SHA-256"}
}

func generateNonce(length int) []byte {
	nonce := make([]byte, length)
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(nonce)))
	// golang stdlib rand.Read function reads random bytes from a global
	// Reader which is a shared instance of a cryptographically secure
	// random number generator. More details can be found at
	// https://pkg.go.dev/crypto/rand#Read
	rand.Read(nonce)
	// Base64 encode the nonce to make it ASCII
	base64.StdEncoding.Encode(dst, nonce)
	return dst

}

func parseSASLData(data []byte) (map[byte][]byte, error) {
	saslData := make(map[byte][]byte)
	parts := bytes.Split(data, []byte{','})
	for _, part := range parts {
		// Each part is of the form single byte key, '=', value
		// Example: 'r=clientNonce'
		key := part[0]
		value := part[2:]
		saslData[key] = value
	}

	return saslData, nil
}

func scramSaltedPassword(password []byte, salt []byte, iterations int, ctx *SaslContext) []byte {
	one := []byte{0, 0, 0, 1}

	// Write the pass, salt, and one to the HMAC
	mac := hmac.New(ctx.hashFunc, password)
	mac.Write(salt)
	mac.Write(one)
	unInitPrev := mac.Sum(nil)

	// Do not need to copy the slice since we dont modify
	// unInitPrev we just replace its with another slice
	result := unInitPrev

	// iterate for the remaining iterations - 1 times
	for i := 1; i < iterations; i++ {
		mac.Reset()
		mac.Write(unInitPrev)
		unInitPrev = mac.Sum(nil)
		for j := 0; j < SHA256_BLOCK_SIZE; j++ {
			result[j] ^= unInitPrev[j]
		}
	}
	return result
}

func scramClientKey(saltedPassword []byte) []byte {
	mac := hmac.New(sha256.New, saltedPassword)
	mac.Write([]byte("Client Key"))
	return mac.Sum(nil)
}

func buildServerFirstMessage(
	salt []byte,
	iterations int,
	fullNonce []byte,
) []byte {
	// convert the iterations to a byte slice
	iterationsBytes := []byte(strconv.Itoa(iterations))
	// Calculate the length of the whole server first message
	msgLen := 2 + len(fullNonce) + 3 + len(salt) + 3 + len(iterationsBytes)
	// Allocate a buffer for the server first message
	serverFirstMessage := make([]byte, msgLen)
	// Build the server first message
	idx := 0
	idx = parsing.WriteBytes(serverFirstMessage, idx, []byte("r="))
	idx = parsing.WriteBytes(serverFirstMessage, idx, fullNonce)
	idx = parsing.WriteBytes(serverFirstMessage, idx, []byte(",s="))
	idx = parsing.WriteBytes(serverFirstMessage, idx, salt)
	idx = parsing.WriteBytes(serverFirstMessage, idx, []byte(",i="))
	idx = parsing.WriteBytes(serverFirstMessage, idx, iterationsBytes)

	return serverFirstMessage
}

func initializeProofMessage(serverNonce []byte) (int, []byte) {
	// 256 bytes should be more than enough for the response
	// but we will use WriteBytesSafe to ensure the buffer i
	// expanded if needed
	saslResponseBuffer := make([]byte, 256)

	idx := 0
	// "c=biws" -> base64("n,,n=")ntNonce
	// Write initial data plus the server nonce
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte("c=biws,r="))
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, serverNonce)

	return idx, saslResponseBuffer
}

func buildClientBareFirstMessage(clientNonce []byte) []byte {
	clientFirstMessageBare := []byte("n=,r=")
	clientFirstMessageBare = append(clientFirstMessageBare, clientNonce...)
	return clientFirstMessageBare[:len(clientNonce)+5]
}

func verifyServerSignature(serverSignature []byte, ctx *SaslContext) bool {
	// TODO: Implement server signature verification
	return true
}

func calculateClientProof(
	saltedPassword []byte,
	iterations int,
	clientBareFirstMessage []byte,
	serverFirstMessage []byte,
	messageWithoutProof []byte,
	ctx *SaslContext,
) []byte {
	// Calculate the client key from the salted password
	clientKey := scramClientKey(saltedPassword)

	// Calculate the stored key from the client key
	hash := ctx.hashFunc()
	hash.Write(clientKey)
	storedKey := hash.Sum(nil)

	// Start a new HMAC with the stored key as its key
	mac := hmac.New(ctx.hashFunc, storedKey)

	// Write the original message without channel binding info
	mac.Write(clientBareFirstMessage)

	// Write a comma
	mac.Write([]byte{','})

	// Write the server first message
	mac.Write(serverFirstMessage)

	// Write a comma
	mac.Write([]byte{','})

	// Write the message without the proof
	mac.Write(messageWithoutProof)

	// Sum to get the client signature
	clientSignature := mac.Sum(nil)

	// XOR the client key with the client signature
	for i := 0; i < len(clientKey); i++ {
		clientKey[i] ^= clientSignature[i]
	}

	// Base64 encode the client key to get the client proof
	clientProof := make([]byte, base64.StdEncoding.EncodedLen(len(clientKey)))
	base64.StdEncoding.Encode(clientProof, clientKey)

	return clientProof

}

type SaslContext struct {
	// The SASL mechanism to use
	hashFunc func() hash.Hash
	// The client nonce
	clientNonce []byte
	// The server nonce
	serverNonce []byte
	// The salt from the server
	salt []byte
	// The number of iterations to use
	iterations int
	// The salted password
	saltedPassword []byte
	// client first message without channel binding
	clientFirstMessageBare []byte
	// sever first response
	serverFirstResponse []byte
	// client challenge response without the proof
	clientChallengeResponseWithoutProof []byte
	// client proof
	clientProof []byte
}

func handleSASLIntitialRequest(
	conn net.Conn,
	rawMessage *protocol.RawPgMessage,
	ctx *SaslContext,
) ([]byte, error) {
	authSASLRequest := &protocol.AuthenticationSASLPgMessage{}
	authSASLRequest, err := authSASLRequest.Unpack(rawMessage)
	if err != nil {
		return []byte{}, err
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
	switch chosenMechanism {
	case "SCRAM-SHA-256":
		ctx.hashFunc = sha256.New
	default:
		return []byte{}, errors.New("No supported SASL mechanism")
	}

	// formulate the scram-sha-256 initial response
	//
	// We do not support channel binding
	idx = parsing.WriteBytes(responseData, idx, []byte("n,,"))
	noBindingIdx := idx

	// Usernam is left blank since the sever will use the username
	// from the intitial startup message. This avoids the need to
	// run SASLprep on the username
	idx = parsing.WriteBytes(responseData, idx, []byte("n="))

	// Write the base64 encoded client Nonce
	clientNonce := generateNonce(18)
	idx = parsing.WriteBytes(responseData, idx, []byte(",r="))
	idx = parsing.WriteBytes(responseData, idx, clientNonce)
	responseData = responseData[:idx]

	// Update the context
	ctx.clientNonce = clientNonce
	ctx.clientFirstMessageBare = responseData[noBindingIdx:]

	saslInitialResponse := protocol.BuildSASLInitialResponseMessage(chosenMechanism, responseData)
	conn.Write(saslInitialResponse.Pack())

	return clientNonce, nil
}

func handleSASLContinue(
	conn net.Conn,
	rawMessage *protocol.RawPgMessage,
	clientNonce []byte,
	password string,
	ctx *SaslContext,
) error {
	// record the server first response
	ctx.serverFirstResponse = bytes.TrimSpace(bytes.Trim(rawMessage.Data, "\x00"))

	authSASLContinue := &protocol.AuthenticationSASLContinuePgMessage{}
	authSASLContinue, err := authSASLContinue.Unpack(rawMessage)
	if err != nil {
		slog.Error("Error unpacking SASL continue message")
		return err
	}

	saslDataMap, err := parseSASLData(authSASLContinue.Data)
	if err != nil {
		return err
	}

	// Verify the server nonce
	serverNonce := saslDataMap['r']
	if !bytes.HasPrefix(serverNonce, clientNonce) {
		slog.Error("Failed SASL auth, server nonce does not match client nonce")
		return errors.New("Server nonce does not match client nonce")
	}
	// record the server nonce
	ctx.serverNonce = serverNonce

	// Decode the server salt
	encodedSalt := saslDataMap['s']
	serverSalt := make([]byte, base64.StdEncoding.DecodedLen(len(encodedSalt)))
	_, err = base64.StdEncoding.Decode(serverSalt, encodedSalt)
	if err != nil {
		slog.Error("Failed to decode server salt")
		return err
	}
	decodedSalt := bytes.Trim(serverSalt, "\x00")
	// record the salt
	ctx.salt = decodedSalt

	serverIterations, err := strconv.Atoi(string(saslDataMap['i']))
	if err != nil {
		slog.Error("Failed to parse server iterations")
		return err
	}
	// record the iterations
	ctx.iterations = serverIterations

	idx, saslResponseBuffer := initializeProofMessage(serverNonce)

	// Copy the client challenge response without the proof
	// since we will still need to update the saslResponseBuffer
	ctx.clientChallengeResponseWithoutProof = make([]byte, idx)
	copy(ctx.clientChallengeResponseWithoutProof, saslResponseBuffer[:idx])

	// Prep the password for the SCRAM-SHA-256 algorithm
	saslPreppedPassword := saslPrep(password)
	// Salt the password `iterations` times (usually 4096)
	saltedPassword := scramSaltedPassword(
		saslPreppedPassword,
		decodedSalt,
		serverIterations,
		ctx,
	)
	// record the salted password
	ctx.saltedPassword = saltedPassword

	// Calculate the client proof
	clientProof := calculateClientProof(
		saltedPassword,
		serverIterations,
		ctx.clientFirstMessageBare,
		ctx.serverFirstResponse,
		saslResponseBuffer,
		ctx,
	)
	ctx.clientProof = clientProof

	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte(",p="))
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte(clientProof))

	saslResponseBuffer = saslResponseBuffer[:idx]

	authSASLResponse := protocol.BuildSASLResponseMessage(saslResponseBuffer)
	conn.Write(authSASLResponse.Pack())

	return nil
}

func handleSASLFinal(conn net.Conn, rawMessage *protocol.RawPgMessage, ctx *SaslContext) error {
	authSASLFinal := &protocol.AuthenticationSASLFinalPgMessage{}
	authSASLFinal, err := authSASLFinal.Unpack(rawMessage)
	if err != nil {
		slog.Error("Error unpacking SASL final message")
		return err
	}
	serverData, err := parseSASLData(authSASLFinal.Data)
	if err != nil {
		slog.Error("Error parsing SASL Data in final message")
		return err
	}

	if len(serverData) != 1 {
		return errors.New("Server returned an invalid SASL final message")
	}

	if mapContainsKey[byte, []byte](serverData, 'e') {
		slog.Error("Server returned an error in the final SASL message")
		return errors.New(string(serverData['e']))
	}

	serverSignature := serverData['v']
	if !verifyServerSignature(serverSignature, ctx) {
		return errors.New("Server signature verification failed")
	}

	return nil
}

func handleSASLAuth(
	conn net.Conn,
	clusterConfig *ClusterConfig,
	rawMessage *protocol.RawPgMessage,
) error {

	ctx := &SaslContext{}
	currentState := "BuildIntialResponse"
	// Save the client nonce to verify the server's response
	clientNonce, err := handleSASLIntitialRequest(conn, rawMessage, ctx)
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
				err = handleSASLContinue(conn, rawMessage, clientNonce, password, ctx)
				if err != nil {
					slog.Error("Error calculating client Response to challenge", "state", currentState)
					return err
				}
				currentState = "ReadSaslFinal"
				// back to the top of the loop
			case protocol.AUTH_SASL_FINAL:
				currentState = "BuildSASLFinal"
				err = handleSASLFinal(conn, rawMessage, ctx)
				if err != nil {
					slog.Error("Error handling SASL final message", "state", currentState)
					return err
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
) error {
	_, authIndicator := parsing.ParseInt32(rawMessage.Data, 0)

	switch authIndicator {
	case protocol.AUTH_OK:
		// No authentication required
	case protocol.AUTH_MD5_PASSWORD:
		handleMD5Auth(conn, clusterConfig, rawMessage)
	case protocol.AUTH_SASL:
		err := handleSASLAuth(conn, clusterConfig, rawMessage)
		if err != nil {
			slog.Error("Error handling SASL authentication", "error", err)
			errMsg := protocol.BuildErrorResponsePgMessage(
				map[string]string{
					"Localized Severity":    "Error",
					"Nonlocalized Severity": "ERROR",
					"Message":               "Server side SASL authentication failed",
					"Detail":                err.Error(),
					"Code":                  "08000",
					"Hint":                  "Check the pgspanner server logs for more information",
					"Routine":               "handleServerAuth",
				},
			)
			return errMsg
		}
	default:
		slog.Error("Unsupported authentication type", "indicator", authIndicator)
		errMsg := protocol.BuildErrorResponsePgMessage(
			map[string]string{
				"Localized Severity":    "Error",
				"Nonlocalized Severity": "ERROR",
				"Message":               "Unknown server authentication type",
				"Detail":                fmt.Sprintf("Recieved message with authtype of: %d", authIndicator),
				"Code":                  "28000",
				"Hint":                  "Check the pgspanner server logs for more information",
				"Routine":               "handleServerAuth",
			},
		)
		return errMsg
	}
	return nil
}
