package protocol

// A Interface for message types in the postgres protocol
type Message interface {
	Unpack(*RawMessage) (*Message, error)
	Pack() []byte
}
