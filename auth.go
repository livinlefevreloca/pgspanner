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

// Functions for handling authentication with the server
func saslPrep(input string) []byte {
	return []byte(input)
}

// We only support SCRAM-SHA-256 for now
func getSupportedSASLMechanisms() []string {
	return []string{"SCRAM-SHA-256"}
}

// Parse the integer from an auth message to determine the type of authentication
func parseAuthIndicator(rawMessage *protocol.RawPgMessage) int {
	idx, authIndicator := parsing.ParseInt32(rawMessage.Data, 0)
	rawMessage.Data = rawMessage.Data[idx:]
	return authIndicator
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

// Parse the data out of a SASL message
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

func scramClientKey(saltedPassword []byte, hashFunc func() hash.Hash) []byte {
	mac := hmac.New(hashFunc, saltedPassword)
	mac.Write([]byte("Client Key"))
	return mac.Sum(nil)
}

func scramServerKey(saltedPassword []byte, hashFunc func() hash.Hash) []byte {
	mac := hmac.New(hashFunc, saltedPassword)
	mac.Write([]byte("Server Key"))
	return mac.Sum(nil)
}

func initializeProofMessage(serverNonce []byte) (int, []byte) {
	// 256 bytes should be more than enough for the response
	// but we will use WriteBytesSafe to ensure the buffer i
	// expanded if needed
	saslResponseBuffer := make([]byte, 256)

	idx := 0
	// "c=biws" -> base64("n,,n=")
	// Write initial data plus the server nonce
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte("c=biws,r="))
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, serverNonce)

	return idx, saslResponseBuffer
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

func (ctx *SaslContext) scramSaltedPassword(password []byte) {
	one := []byte{0, 0, 0, 1}

	// Write the pass, salt, and one serverSignatureto the HMAC
	mac := hmac.New(ctx.hashFunc, password)
	mac.Write(ctx.salt)
	mac.Write(one)
	unInitPrev := mac.Sum(nil)

	// Do not need to copy the slice since we dont modify
	// unInitPrev we just replace its with another slice
	result := unInitPrev

	// iterate for the remaining iterations - 1 times
	for i := 1; i < ctx.iterations; i++ {
		mac.Reset()
		mac.Write(unInitPrev)
		unInitPrev = mac.Sum(nil)
		for j := 0; j < SHA256_BLOCK_SIZE; j++ {
			result[j] ^= unInitPrev[j]
		}
	}

	ctx.saltedPassword = result
}

func (ctx *SaslContext) calculateServerSignature() []byte {
	// Calculate the server key from the salted password
	serverKey := scramServerKey(ctx.saltedPassword, ctx.hashFunc)

	// Calculate the sever signature
	mac := hmac.New(ctx.hashFunc, serverKey)
	mac.Write(ctx.clientFirstMessageBare)
	mac.Write([]byte{','})
	mac.Write(ctx.serverFirstResponse)
	mac.Write([]byte{','})
	mac.Write(ctx.clientChallengeResponseWithoutProof)
	serverSignatureExpected := mac.Sum(nil)

	b64ServerSignatureExpected := make([]byte, base64.StdEncoding.EncodedLen(len(serverSignatureExpected)))
	base64.StdEncoding.Encode(b64ServerSignatureExpected, serverSignatureExpected)

	return b64ServerSignatureExpected
}

func (ctx *SaslContext) calculateClientProof() {
	// Calculate the client key from the salted password
	clientKey := scramClientKey(ctx.saltedPassword, ctx.hashFunc)

	// Calculate the stored key from the client key
	hash := ctx.hashFunc()
	hash.Write(clientKey)
	storedKey := hash.Sum(nil)

	// Calculate the client signature
	mac := hmac.New(ctx.hashFunc, storedKey)
	mac.Write(ctx.clientFirstMessageBare)
	mac.Write([]byte{','})
	mac.Write(ctx.serverFirstResponse)
	mac.Write([]byte{','})
	mac.Write(ctx.clientChallengeResponseWithoutProof)
	clientSignature := mac.Sum(nil)

	// Calculate the client proof from the client key and the client signature
	for i := 0; i < len(clientKey); i++ {
		clientKey[i] ^= clientSignature[i]
	}

	clientProof := make([]byte, base64.StdEncoding.EncodedLen(len(clientKey)))
	base64.StdEncoding.Encode(clientProof, clientKey)

	ctx.clientProof = clientProof
}

func handleSASLIntitialRequest(conn net.Conn, rawMessage *protocol.RawPgMessage, ctx *SaslContext) error {
	authSASLRequest := &protocol.AuthenticationSASLPgMessage{}
	authSASLRequest, err := authSASLRequest.Unpack(rawMessage)
	if err != nil {
		return err
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
		return errors.New("No supported SASL mechanism")
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
	ctx.clientNonce = generateNonce(18)
	idx = parsing.WriteBytes(responseData, idx, []byte(",r="))
	idx = parsing.WriteBytes(responseData, idx, ctx.clientNonce)
	responseData = responseData[:idx]

	// Save the client first message without channel binding info
	ctx.clientFirstMessageBare = responseData[noBindingIdx:]

	saslInitialResponse := protocol.BuildSASLInitialResponseMessage(chosenMechanism, responseData)
	conn.Write(saslInitialResponse.Pack())

	return nil
}

func handleSASLContinue(
	conn net.Conn,
	rawMessage *protocol.RawPgMessage,
	password string,
	ctx *SaslContext,
) error {
	// record the server first response. The raw message
	ctx.serverFirstResponse = rawMessage.Data

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
	ctx.serverNonce = saslDataMap['r']
	if !bytes.HasPrefix(ctx.serverNonce, ctx.clientNonce) {
		slog.Error("Failed SASL auth, server nonce does not match client nonce")
		return errors.New("Server nonce does not match client nonce")
	}

	// Decode the server salt
	encodedSalt := saslDataMap['s']
	serverSalt := make([]byte, base64.StdEncoding.DecodedLen(len(encodedSalt)))
	_, err = base64.StdEncoding.Decode(serverSalt, encodedSalt)
	if err != nil {
		slog.Error("Failed to decode server salt")
		return err
	}
	ctx.salt = bytes.Trim(serverSalt, "\x00")

	ctx.iterations, err = strconv.Atoi(string(saslDataMap['i']))
	if err != nil {
		slog.Error("Failed to parse server iterations")
		return err
	}

	idx, saslResponseBuffer := initializeProofMessage(ctx.serverNonce)

	// Copy the client challenge response without the proof
	// since we will still need to update the saslResponseBuffer
	ctx.clientChallengeResponseWithoutProof = make([]byte, idx)
	copy(ctx.clientChallengeResponseWithoutProof, saslResponseBuffer[:idx])

	// Prep the password for the SCRAM-SHA-256 algorithm
	saslPreppedPassword := saslPrep(password)
	// Salt the password `iterations` times (usually 4096)
	ctx.scramSaltedPassword(
		saslPreppedPassword,
	)
	// Calculate the client proof
	ctx.calculateClientProof()

	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte(",p="))
	idx, saslResponseBuffer = parsing.WriteBytesSafe(saslResponseBuffer, idx, []byte(ctx.clientProof))

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

	if _, ok := serverData['e']; !ok {
		slog.Error("Server returned an error in the final SASL message")
		return errors.New(string(serverData['e']))
	}

	serverSignature := serverData['v']
	expectedServerSignature := ctx.calculateServerSignature()
	if !bytes.Equal(serverSignature, expectedServerSignature) {
		return errors.New("Server signature verification failed")
	}
	slog.Debug("Server signature verification passed")

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
	err := handleSASLIntitialRequest(conn, rawMessage, ctx)
	if err != nil {
		slog.Error("Error handling SASL initial request", "state", currentState)
		return err
	}
	currentState = "ReadSaslContinue"

	for {
		rawMessage, err = protocol.GetRawPgMessage(conn)
		if err != nil {
			slog.Error("Error getting SASL message from server", "state", currentState)
			return err
		}

		var authIndicator int
		switch rawMessage.Kind {
		case protocol.BMESSAGE_AUTH:
			authIndicator = parseAuthIndicator(rawMessage)
			if err != nil {
				slog.Error("Error reading SASL authentication indicator", "state", currentState)
				return err
			}
			switch authIndicator {
			case protocol.AUTH_SASL_CONTINUE:
				currentState = "BuildSASLResponse"
				password := os.Getenv(clusterConfig.PasswordEnv)
				err = handleSASLContinue(conn, rawMessage, password, ctx)
				if err != nil {
					slog.Error("Error calculating client Response to challenge", "state", currentState)
					return err
				}
				currentState = "ReadSaslFinal"
				// back to the top of the loop
				break
			case protocol.AUTH_SASL_FINAL:
				currentState = "BuildSASLFinal"
				err = handleSASLFinal(conn, rawMessage, ctx)
				if err != nil {
					slog.Error("Error handling SASL final message", "state", currentState)
					return err
				}
				slog.Debug("SASL authentication complete")
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
			slog.Error("Error response from server in SASL", "state", currentState)
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

func handleMD5Auth(conn net.Conn, clusterConfig *ClusterConfig, rawMessage *protocol.RawPgMessage) error {
	md5Message := &protocol.AuthenticationMD5PasswordPgMessage{}
	md5Message, err := md5Message.Unpack(rawMessage)
	if err != nil {
		slog.Error("Error unpacking MD5 authentication message")
		return err
	}

	password := os.Getenv(clusterConfig.PasswordEnv)
	md5Password := getMd5Password(password, clusterConfig.User, md5Message.Salt)
	md5PasswordMessage := protocol.BuildPasswordMessage(md5Password)
	conn.Write(md5PasswordMessage.Pack())

	rawMessage, err = protocol.GetRawPgMessage(conn)
	if err != nil {
		slog.Error("Error reading MD5 authentication response")
		return err
	}

	switch rawMessage.Kind {
	case protocol.BMESSAGE_AUTH:
		authIndicator := parseAuthIndicator(rawMessage)
		switch authIndicator {
		case protocol.AUTH_OK:
			break
		default:
			slog.Error("Unexpected MD5 authentication response", "indicator", authIndicator)
			return protocol.MakeConnectionErrorMessages(
				"Unexpected MD5 authentication response",
				fmt.Sprintf("Recieved message with indicator of: %d", authIndicator),
				"08000",
				"handleMD5Auth",
			)
		}
		break
	case protocol.BMESSAGE_ERROR_RESPONSE:
		serverError := &protocol.ErrorResponsePgMessage{}
		serverError, err = serverError.Unpack(rawMessage)
		slog.Error("Error response from server in MD5 authentication")
		return protocol.MakeConnectionErrorMessages(
			"Server side MD5 authentication failed",
			serverError.Error(),
			serverError.GetErrorResponseField(protocol.NOTICE_KIND_CODE),
			"handleMD5Auth",
		)
	default:
		slog.Error("Unexpected message type", "kind", rawMessage.Kind)
		return protocol.MakeConnectionErrorMessages(
			"Unexpected message type in MD5 authentication",
			fmt.Sprintf("Recieved message with kind of: %d", rawMessage.Kind),
			"08000",
			"handleMD5Auth",
		)
	}

	return nil
}

func handleServerAuth(
	conn net.Conn,
	clusterConfig *ClusterConfig,
	rawMessage *protocol.RawPgMessage,
) error {

	authIndicator := parseAuthIndicator(rawMessage)
	var errMsg *protocol.ErrorResponsePgMessage
	var ok bool

	switch authIndicator {
	case protocol.AUTH_OK:
		break
	case protocol.AUTH_MD5_PASSWORD:
		err := handleMD5Auth(conn, clusterConfig, rawMessage)
		if err != nil {
			slog.Error("Error handling MD5 authentication", "error", err)
			errMsg, ok = err.(*protocol.ErrorResponsePgMessage)
			if !ok {
				return protocol.MakeConnectionErrorMessages(
					"Server side MD5 authentication failed",
					err.Error(),
					"08000",
					"handleServerAuth",
				)
			}
			return errMsg
		}
		slog.Info("MD5 authentication complete")
	case protocol.AUTH_SASL:
		err := handleSASLAuth(conn, clusterConfig, rawMessage)
		if err != nil {
			slog.Error("Error handling SASL authentication", "error", err)
			errMsg, ok = err.(*protocol.ErrorResponsePgMessage)
			if !ok {
				return protocol.MakeConnectionErrorMessages(
					"Server side SASL authentication failed",
					err.Error(),
					"08000",
					"handleServerAuth",
				)
			}
			return errMsg
		}
	default:
		slog.Error("Unsupported authentication type", "indicator", authIndicator)
		return protocol.MakeConnectionErrorMessages(
			"Unkown authentication type",
			fmt.Sprintf("Recieved message with authtype of: %d", authIndicator),
			"08000",
			"handleServerAuth",
		)

	}
	return nil
}
