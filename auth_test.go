package main

import (
	"bytes"
	"crypto"
	"fmt"
	"testing"
)

func TestInitializeProofMessage(t *testing.T) {
	serverNonce := []byte("C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	expectedClientMesageWithoutProof := []byte("c=biws,r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	_, clientMessageWithoutProof := initializeProofMessage(serverNonce)
	if !matchesString(string(clientMessageWithoutProof), string(expectedClientMesageWithoutProof)) {
		t.Fatal("clientMessageWithoutProof does not match")
	}
}

func TestSaltedPassword(t *testing.T) {
	clientNonce := []byte("C4KQWksX6Hr693gst2i+4ET5")
	serverNonce := []byte("C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")

	salt := []byte("NqkjGpyJLsb2TRd/vhu8pg==")
	decodedLen := b64.DecodedLen(len(salt))
	decodedSalt := make([]byte, decodedLen)
	b64.Decode(decodedSalt, salt)
	decodedSalt = bytes.Trim(decodedSalt, "\x00")

	ctx := &SaslContext{
		hashFunc:    crypto.SHA256.New,
		clientNonce: clientNonce,
		serverNonce: serverNonce,
		salt:        decodedSalt,
		iterations:  4096,
	}

	expectedSaltedPassword := [...]byte{254, 117, 22, 22, 156, 185, 210, 138, 143, 61, 153, 127, 109, 112, 179, 150, 145, 62, 147, 130, 75, 222, 71, 204, 16, 39, 144, 234, 110, 103, 22, 29}
	ctx.scramSaltedPassword([]byte("root"))
	if !matchesBytes(ctx.saltedPassword, expectedSaltedPassword[:]) {
		t.Fatal("Salted password did not match")
	}
}

func TestClientProof(t *testing.T) {

	clientFirstMessageBare := []byte("n=,r=C4KQWksX6Hr693gst2i+4ET5")
	serverFirstMessage := []byte("r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN,s=NqkjGpyJLsb2TRd/vhu8pg==,i=4096")
	clientMesageWithoutProof := []byte("c=biws,r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	clientNonce := []byte("C4KQWksX6Hr693gst2i+4ET5")
	serverNonce := []byte("C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	saltedPassword := [...]byte{254, 117, 22, 22, 156, 185, 210, 138, 143, 61, 153, 127, 109, 112, 179, 150, 145, 62, 147, 130, 75, 222, 71, 204, 16, 39, 144, 234, 110, 103, 22, 29}

	salt := []byte("NqkjGpyJLsb2TRd/vhu8pg==")
	decodedLen := b64.DecodedLen(len(salt))
	decodedSalt := make([]byte, decodedLen)
	b64.Decode(decodedSalt, salt)
	decodedSalt = bytes.Trim(decodedSalt, "\x00")

	ctx := &SaslContext{
		hashFunc:                            crypto.SHA256.New,
		clientNonce:                         clientNonce,
		serverNonce:                         serverNonce,
		salt:                                decodedSalt,
		iterations:                          4096,
		clientFirstMessageBare:              clientFirstMessageBare,
		serverFirstResponse:                 serverFirstMessage,
		clientChallengeResponseWithoutProof: clientMesageWithoutProof,
		saltedPassword:                      saltedPassword[:],
	}

	expectedProof := []byte("oiDCklV4A+KVNGngoJUMXjEwlkrm0md+7gJ81sjrs84=")
	ctx.calculateClientProof()
	if !matchesString(string(ctx.clientProof), string(expectedProof)) {
		t.Fatal("Proofs did not match")
	}
}

func TestVerifyServerSignature(t *testing.T) {
	clientFirstMessageBare := []byte("n=,r=C4KQWksX6Hr693gst2i+4ET5")
	serverFirstMessage := []byte("r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN,s=NqkjGpyJLsb2TRd/vhu8pg==,i=4096")
	ClientMesageWithoutProof := []byte("c=biws,r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	clientNonce := []byte("C4KQWksX6Hr693gst2i+4ET5")
	serverNonce := []byte("C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	salt := []byte("NqkjGpyJLsb2TRd/vhu8pg==")
	saltedPassword := [...]byte{254, 117, 22, 22, 156, 185, 210, 138, 143, 61, 153, 127, 109, 112, 179, 150, 145, 62, 147, 130, 75, 222, 71, 204, 16, 39, 144, 234, 110, 103, 22, 29}

	decodedLen := b64.DecodedLen(len(salt))
	decodedSalt := make([]byte, decodedLen)
	b64.Decode(decodedSalt, salt)
	decodedSalt = bytes.Trim(decodedSalt, "\x00")

	ctx := &SaslContext{
		hashFunc:                            crypto.SHA256.New,
		clientNonce:                         clientNonce,
		serverNonce:                         serverNonce,
		salt:                                decodedSalt,
		iterations:                          4096,
		clientFirstMessageBare:              clientFirstMessageBare,
		serverFirstResponse:                 serverFirstMessage,
		clientChallengeResponseWithoutProof: ClientMesageWithoutProof,
		saltedPassword:                      saltedPassword[:],
	}

	expectedServerSignature := []byte("c6BaPXTCrNU+cq37GaucEsrnDMjZcWqcdGmnQd/lwaA=")
	serverSignature := ctx.calculateServerSignature()
	if !matchesString(string(serverSignature), string(expectedServerSignature)) {
		t.Fatal("Server signature did not match")
	}

}

func matchesBytes(left []byte, right []byte) bool {
	if !bytes.Equal(left, right) {
		printBytesAsHex("Got (left)", left)
		printBytesAsHex("Expected (right)", right)
		return false
	}

	return true
}

func matchesString(left string, right string) bool {
	if left != right {
		fmt.Printf("Got (left): %s\n", left)
		fmt.Printf("Expected (right): %s\n", right)
		return false
	}

	return true
}
