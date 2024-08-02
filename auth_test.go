package main

import (
	"bytes"
	"fmt"
	"testing"
)

func printBytesAsHex(tag string, bytes []byte) {
	fmt.Printf("%s:", tag)
	for _, b := range bytes {
		fmt.Printf("\\x%02x", b)
	}
	fmt.Println()
}

func TestClientProof(t *testing.T) {
	serverFirstMessage := []byte("r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN,s=NqkjGpyJLsb2TRd/vhu8pg==,i=4096")
	clientNonce := []byte("C4KQWksX6Hr693gst2i+4ET5")
	serverNonce := []byte("C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")

	salt := []byte("NqkjGpyJLsb2TRd/vhu8pg==")
	decodedLen := b64.DecodedLen(len(salt))
	decodedSalt := make([]byte, decodedLen)
	b64.Decode(decodedSalt, salt)
	decodedSalt = bytes.Trim(decodedSalt, "\x00")

	expectedBareFirstMessage := []byte("n=,r=C4KQWksX6Hr693gst2i+4ET5")
	bareFirstMessage := buildClientBareFirstMessage(clientNonce)
	if !matches(bareFirstMessage, expectedBareFirstMessage) {
		t.Fatal("Bare First Message does not match")
	}

	expectedClientMesageWithoutProof := []byte("c=biws,r=C4KQWksX6Hr693gst2i+4ET5C0dywTDp77Sa5H1DrXzlYGNN")
	_, clientMessageWithoutProof := initializeProofMessage(serverNonce)
	if !matches(clientMessageWithoutProof, expectedClientMesageWithoutProof) {
		t.Fatal("clientMessageWithoutProof does not match")
	}

	expectedSaltedPassword := [...]byte{254, 117, 22, 22, 156, 185, 210, 138, 143, 61, 153, 127, 109, 112, 179, 150, 145, 62, 147, 130, 75, 222, 71, 204, 16, 39, 144, 234, 110, 103, 22, 29}
	saltedPassword := scramSaltedPassword([]byte("root"), decodedSalt, 4096)
	if !matches(saltedPassword, expectedSaltedPassword[:]) {
		t.Fatal("Salted password did not match")
	}

	expectedProof := []byte("oiDCklV4A+KVNGngoJUMXjEwlkrm0md+7gJ81sjrs84=")
	proof := calculateClientProof(
		saltedPassword,
		4096,
		bareFirstMessage,
		serverFirstMessage,
		clientMessageWithoutProof,
	)
	if !matches(proof, expectedProof) {
		t.Fatal("Proofs did not match")
	}
}

func matches(left []byte, right []byte) bool {
	if !bytes.Equal(left, right) {
		printBytesAsHex("Got (left)", left)
		printBytesAsHex("Expected (right)", right)
		return false
	}

	return true
}
