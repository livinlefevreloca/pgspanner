package protocol

import "github.com/livinlefevreloca/pgspanner/utils"

// Backend Postgres Message kinds
const (
	BMESSAGE_ROW_DESCRIPTION  = 84
	BMESSAGE_AUTH             = 82
	BMESSAGE_PARAMETER_STATUS = 83
	BMESSAGE_BACKEND_KEY_DATA = 75
	BMESSAGE_READY_FOR_QUERY  = 90
	BMESSAGE_NO_DATA          = 110
	BMESSAGE_DATA_ROW         = 68
)

const (
	AUTH_OK           = 0
	AUTH_MD5_PASSWORD = 5
)

// AuthenticationOkPgMessage represents the message sent by the server to indicate that the authentication was successful
type AuthenticationOkPgMessage struct{}

// Build a new AuthenticationOkPgMessage
func BuildAuthenticationOkPgMessage() *AuthenticationOkPgMessage {
	return &AuthenticationOkPgMessage{}
}

// Postgres Message interface implementation for AuthenticationOkPgMessage
func (m *AuthenticationOkPgMessage) Unpack(message *RawPgMessage) (*AuthenticationOkPgMessage, error) {
	return &AuthenticationOkPgMessage{}, nil
}

func (m *AuthenticationOkPgMessage) Pack() []byte {
	messageLength := 4 + 4               // length + null
	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_AUTH))
	idx = utils.WriteInt32(out, 1, messageLength)
	utils.WriteInt32(out, idx, 0)

	return out
}

type AuthenticationMD5PasswordPgMessage struct {
	inidicator int
	Salt       []byte
}

func BuildAuthenticationMD5PasswordPgMessage(salt []byte) *AuthenticationMD5PasswordPgMessage {
	return &AuthenticationMD5PasswordPgMessage{5, salt}
}

// Postgres Message interface implementation for AuthenticationMD5PasswordPgMessage
func (m *AuthenticationMD5PasswordPgMessage) Unpack(message *RawPgMessage) (*AuthenticationMD5PasswordPgMessage, error) {
	idx := 0
	idx, m.inidicator = utils.ParseInt32(message.Data, idx)
	idx, m.Salt = utils.ParseBytes(message.Data, idx, 4)

	return m, nil
}

func (m *AuthenticationMD5PasswordPgMessage) Pack() []byte {
	messageLength := 4 + 4 + 4 // length + indicator + salt
	out := make([]byte, messageLength+1)

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_AUTH))
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteInt32(out, idx, m.inidicator)
	utils.WriteBytes(out, idx, m.Salt)

	return out
}

// RowDescriptionPgMessage represents the message sent by the server to describe the fields of a row
type RowDescriptionPgMessage struct {
	Fields []FieldDescription
}

// Build a new RowDescriptionPgMessage
func BuildRowDescriptionPgMessage(fieldsMap map[string][]int) *RowDescriptionPgMessage {
	var fields []FieldDescription
	for name, fieldData := range fieldsMap {
		field := buildFieldDescription(
			name,
			fieldData[0],
			fieldData[1],
			fieldData[2],
			fieldData[3],
			fieldData[4],
			fieldData[5],
		)
		fields = append(fields, *field)
	}
	return &RowDescriptionPgMessage{fields}
}

// PostgresMessage interface implementation for RowDescriptionPgMessage
func (m *RowDescriptionPgMessage) Unpack(message *RawPgMessage) (*RowDescriptionPgMessage, error) {
	idx := 0
	idx, fieldCount := utils.ParseInt16(message.Data, idx)

	fields := make([]FieldDescription, fieldCount)
	for i := 0; i < fieldCount; i++ {
		consumed, field, err := parseFieldDescription(message.Data[idx:])
		if err != nil {
			return nil, err
		}
		fields[i] = *field
		idx += consumed
	}

	m.Fields = fields

	return m, nil
}

func (m RowDescriptionPgMessage) Pack() []byte {
	messageLength := 4 + 2 // length + field count
	for _, field := range m.Fields {
		messageLength += field.byteLength()
	}

	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	// Write the kind of message
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_ROW_DESCRIPTION))
	// Write the length of the message
	idx = utils.WriteInt32(out, idx, messageLength)
	// Write the number of fields
	idx = utils.WriteInt16(out, idx, len(m.Fields))
	for _, field := range m.Fields {
		fieldBytes := field.Pack()
		idx = utils.WriteBytes(out, idx, fieldBytes)
	}

	return out
}

// FieldDescription represents the description of a field in a row
type FieldDescription struct {
	Name         string
	tableOid     int
	columnOid    int
	typeOid      int
	typeSize     int
	typeModifier int
	format       int
}

func parseFieldDescription(data []byte) (int, *FieldDescription, error) {
	idx := 0

	idx, name, err := utils.ParseCString(data, idx)
	if err != nil {
		return idx, nil, err
	}

	idx, tableOid := utils.ParseInt32(data, idx)
	idx, columnOid := utils.ParseInt16(data, idx)
	idx, typeOid := utils.ParseInt32(data, idx)
	idx, typeSize := utils.ParseInt16(data, idx)
	idx, typeModifier := utils.ParseInt32(data, idx)
	idx, format := utils.ParseInt16(data, idx)

	field := FieldDescription{
		Name:         name,
		tableOid:     tableOid,
		columnOid:    columnOid,
		typeOid:      typeOid,
		typeSize:     typeSize,
		typeModifier: typeModifier,
		format:       format,
	}

	return idx, &field, nil
}

func buildFieldDescription(
	name string,
	tableOid int,
	columnOid int,
	typeOid int,
	typeSize int,
	typeModifier int,
	format int,
) *FieldDescription {
	return &FieldDescription{
		Name:         name,
		tableOid:     tableOid,
		columnOid:    columnOid,
		typeOid:      typeOid,
		typeSize:     typeSize,
		typeModifier: typeModifier,
		format:       format,
	}
}

// DataRowPgMessage represents the message sent by the server to send a row of data
type DataRowPgMessage struct {
	Values [][]byte
}

func BuildDataRowPgMessage(values [][]byte) *DataRowPgMessage {
	return &DataRowPgMessage{values}
}

func (m *DataRowPgMessage) getByteLength() int {
	var messageLength int
	for _, value := range m.Values {
		messageLength += 4 + len(value)
	}
	return messageLength
}

// PostgresMessage interface implementation for DataRowPgMessage
func (m *DataRowPgMessage) Unpack(message *RawPgMessage) (*DataRowPgMessage, error) {
	idx := 0
	idx, rowCount := utils.ParseInt16(message.Data, idx)
	values := make([][]byte, rowCount)
	for i := 0; i < rowCount; i++ {
		idx, valueLength := utils.ParseInt32(message.Data, idx)
		idx, value := utils.ParseBytes(message.Data, idx, valueLength)
		values[i] = value
	}

	return &DataRowPgMessage{values}, nil
}

func (m *DataRowPgMessage) Pack() []byte {
	messageLength := m.getByteLength() + 2 + 4 // content + column count + length
	out := make([]byte, messageLength+1)       // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_DATA_ROW))
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteInt16(out, idx, len(m.Values))
	for _, value := range m.Values {
		idx = utils.WriteInt32(out, idx, len(value))
		idx = utils.WriteBytes(out, idx, value)
	}

	return out
}

// Return the length of the field description in bytes
func (d FieldDescription) byteLength() int {
	// name + null terminator + table oid + column oid + type oid + type size + type modifier + format
	return len(d.Name) + 1 + 4 + 2 + 4 + 2 + 4 + 2
}

func (d FieldDescription) Pack() []byte {
	messageLength := d.byteLength()
	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	// Write the name of the field
	idx = utils.WriteCString(out, idx, d.Name)
	// Write the table oid
	idx = utils.WriteInt32(out, idx, d.tableOid)
	// Write the column oid
	idx = utils.WriteInt16(out, idx, d.columnOid)
	// Write the type oid
	idx = utils.WriteInt32(out, idx, d.typeOid)
	// Write the type size
	idx = utils.WriteInt16(out, idx, d.typeSize)
	// Write the type modifier
	idx = utils.WriteInt32(out, idx, d.typeModifier)
	// Write the format
	utils.WriteInt16(out, idx, d.format)

	return out
}

// ParameterStatusPgMessage represents the message sent by the server to inform the client of sever parameter values
type ParameterStatusPgMessage struct {
	Name  string
	Value string
}

func BuildParameterStatusPgMessage(name string, value string) *ParameterStatusPgMessage {
	return &ParameterStatusPgMessage{name, value}
}

func (m *ParameterStatusPgMessage) getByteLength() int {
	return len(m.Name) + 1 + len(m.Value) + 1
}

// Postgres Message interface implementation for ParameterStatusPgMessage
func (m *ParameterStatusPgMessage) Unpack(message *RawPgMessage) (*ParameterStatusPgMessage, error) {
	idx := 0
	idx, name, err := utils.ParseCString(message.Data, idx)
	if err != nil {
		return nil, err
	}
	idx, value, err := utils.ParseCString(message.Data, idx)
	if err != nil {
		return nil, err
	}

	return &ParameterStatusPgMessage{name, value}, nil
}

func (m *ParameterStatusPgMessage) Pack() []byte {
	messageLength := m.getByteLength() + 4 // content + length
	out := make([]byte, messageLength+1)   // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_PARAMETER_STATUS))
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteCString(out, idx, m.Name)
	utils.WriteCString(out, idx, m.Value)

	return out
}

// BackendKeyDataPgMessage represents the message sent by the server to inform the client of the process id and secret key
type BackendKeyDataPgMessage struct {
	Pid       int
	SecretKey int
}

func BuildBackendKeyDataPgMessage(processID int, secretKey int) *BackendKeyDataPgMessage {
	return &BackendKeyDataPgMessage{processID, secretKey}
}

// Postgres Message interface implementation for BackendKeyDataPgMessage
func (m *BackendKeyDataPgMessage) Unpack(message *RawPgMessage) (*BackendKeyDataPgMessage, error) {
	idx := 0
	idx, Pid := utils.ParseInt32(message.Data, idx)
	idx, secretKey := utils.ParseInt32(message.Data, idx)

	return &BackendKeyDataPgMessage{Pid, secretKey}, nil
}

func (m *BackendKeyDataPgMessage) Pack() []byte {
	messageLength := 4 + 4 + 4           // length + process id + secret key
	out := make([]byte, messageLength+1) // +1 for the kind of message (1 byte

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_BACKEND_KEY_DATA))
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteInt32(out, idx, m.Pid)
	utils.WriteInt32(out, idx, m.SecretKey)

	return out
}

// ReadyForQueryPgMessage represents the message sent by the server to indicate that the server is ready to accept a new query
type ReadyForQueryPgMessage struct {
	TransactionStatus byte
}

func BuildReadyForQueryPgMessage(status byte) *ReadyForQueryPgMessage {
	return &ReadyForQueryPgMessage{status}
}

// Postgres Message interface implementation for ReadyForQueryPgMessage
func (m *ReadyForQueryPgMessage) Unpack(message *RawPgMessage) (*ReadyForQueryPgMessage, error) {
	return &ReadyForQueryPgMessage{}, nil
}

func (m *ReadyForQueryPgMessage) Pack() []byte {
	messageLength := 4 + 1               // length + status
	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_READY_FOR_QUERY))
	idx = utils.WriteInt32(out, 1, messageLength)
	utils.WriteByte(out, idx, m.TransactionStatus)

	return out
}

// NoDataPgMessage represents the message sent by the server to indicate that there is no data to return
type NoDataPgMessage struct{}

func BuildNoDataPgMessage() *NoDataPgMessage {
	return &NoDataPgMessage{}
}

// Postgres Message interface implementation for NoDataPgMessage
func (m *NoDataPgMessage) Unpack(message *RawPgMessage) (*NoDataPgMessage, error) {
	return &NoDataPgMessage{}, nil
}

func (m *NoDataPgMessage) Pack() []byte {
	messageLength := 4
	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_NO_DATA))
	utils.WriteInt32(out, idx, messageLength)

	return out
}

// CommandCompletePgMessage represents the message sent by the server to indicate that a command has been completed
type CommandCompletePgMessage struct {
	Command string
}

func BuildCommandCompletePgMessage(command string) *CommandCompletePgMessage {
	return &CommandCompletePgMessage{command}
}

// Message interface implementation for CommandCompletePgMessage
func (m *CommandCompletePgMessage) Unpack(message *RawPgMessage) (*CommandCompletePgMessage, error) {
	return &CommandCompletePgMessage{string(message.Data)}, nil
}

func (m *CommandCompletePgMessage) Pack() []byte {
	messageLength := len(m.Command) + 1 + 4 // content + length
	out := make([]byte, messageLength+1)    // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte('C'))
	idx = utils.WriteInt32(out, idx, messageLength)
	utils.WriteCString(out, idx, m.Command)

	return out
}
