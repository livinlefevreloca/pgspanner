package parsing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// Functions to read from a reader
func ReadInt32(reader io.Reader) (int, error) {
	var value int32
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return int(value), nil
}

// Functions to parse from a byte slice
func ParseInt32(data []byte, idx int) (int, int) {
	return idx + 4, int(binary.BigEndian.Uint32(data[idx : idx+4]))
}

func ParseInt16(data []byte, idx int) (int, int) {
	return idx + 2, int(binary.BigEndian.Uint16(data[idx : idx+2]))
}

func ParseCString(data []byte, idx int) (int, string, error) {
	i := bytes.IndexByte(data[idx:], 0)
	if i == -1 {
		return 0, "", errors.New("Invalid string")
	}
	return idx + i + 1, string(data[idx : idx+i]), nil
}

func ParseBytes(data []byte, idx int, length int) (int, []byte) {
	return idx + length, data[idx : idx+length]
}

// Functions to write to a byte slice

func WriteByte(data []byte, idx int, value byte) int {
	data[idx] = value
	return idx + 1
}

func WriteInt32(data []byte, idx int, value int) int {
	binary.BigEndian.PutUint32(data[idx:idx+4], uint32(value))
	return idx + 4
}

func WriteInt16(data []byte, idx int, value int) int {
	binary.BigEndian.PutUint16(data[idx:idx+2], uint16(value))
	return idx + 2
}

func WriteCString(data []byte, idx int, value string) int {
	copy(data[idx:], value)
	data[idx+len(value)] = 0
	return idx + len(value) + 1
}

func WriteBytes(data []byte, idx int, value []byte) int {
	copy(data[idx:], value)
	return idx + len(value)
}

// "SAFE" functions

func WriteByteSafe(data []byte, idx int, value byte) (int, []byte) {
	data = append(data[:idx], value)
	return idx + 1, data
}

func WriteInt32Safe(data []byte, idx int, value int) (int, []byte) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(value))
	data = append(data[:idx], b...)
	return idx + 4, data
}

func WriteInt16Safe(data []byte, idx int, value int) (int, []byte) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(value))
	data = append(data[:idx], b...)
	return idx + 2, data
}

func WriteCStringSafe(data []byte, idx int, value string) (int, []byte) {
	data = append(data[:idx], value...)
	data = append(data, 0)
	return idx + len(value) + 1, data
}

func WriteBytesSafe(data []byte, idx int, value []byte) (int, []byte) {
	data = append(data[:idx], value...)
	return idx + len(value), data
}
