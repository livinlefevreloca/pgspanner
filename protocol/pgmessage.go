package protocol

// A Interface for message types in the postgres protocol
type PgMessage interface {
	Unpack(*RawPgMessage) (*PgMessage, error)
	Pack() []byte
}
