package protocol

import "github.com/livinlefevreloca/pgspanner/utils"

// Backend Message kinds
const (
	BMESSAGE_ROW_DESCRIPTION  = 84
	BMESSAGE_AUTH_OK          = 82
	BMESSAGE_PARAMETER_STATUS = 83
	BMESSAGE_BACKEND_KEY_DATA = 75
	BMESSAGE_READY_FOR_QUERY  = 90
	BMESSAGE_NO_DATA          = 110
	BMESSAGE_DATA_ROW         = 68
)

// AuthenticationOkMessage represents the message sent by the server to indicate that the authentication was successful
type AuthenticationOkMessage struct{}

// Build a new AuthenticationOkMessage
func BuildAuthenticationOkMessage() *AuthenticationOkMessage {
	return &AuthenticationOkMessage{}
}

// Message interface implementation for AuthenticationOkMessage
func (m *AuthenticationOkMessage) Unpack(message *RawMessage) (*AuthenticationOkMessage, error) {
	return &AuthenticationOkMessage{}, nil
}

func (m *AuthenticationOkMessage) Pack() []byte {
	messageLength := 4 + 4               // length + null
	out := make([]byte, messageLength+1) // +1 for the kind of message

	out[0] = byte(BMESSAGE_AUTH_OK)
	idx := utils.WriteInt32(out, 1, messageLength)
	_ = utils.WriteInt32(out, idx, 0)

	return out
}

// RowDescriptionMessage represents the message sent by the server to describe the fields of a row
type RowDescriptionMessage struct {
	Fields []FieldDescription
}

// Build a new RowDescriptionMessage
func BuildRowDescriptionMessage(fieldsMap map[string][]int) *RowDescriptionMessage {
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
	return &RowDescriptionMessage{fields}
}

// Message interface implementation for RowDescriptionMessage
func (m *RowDescriptionMessage) Unpack(message *RawMessage) (*RowDescriptionMessage, error) {
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

	return &RowDescriptionMessage{fields}, nil
}

func (m RowDescriptionMessage) Pack() []byte {
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

// DataRowMessage represents the message sent by the server to send a row of data
type DataRowMessage struct {
	Values [][]byte
}

func BuildDataRowMessage(values [][]byte) *DataRowMessage {
	return &DataRowMessage{values}
}

func (m *DataRowMessage) getByteLength() int {
	var messageLength int
	for _, value := range m.Values {
		messageLength += 4 + len(value)
	}
	return messageLength
}

// Message interface implementation for DataRowMessage
func (m *DataRowMessage) Unpack(message *RawMessage) (*DataRowMessage, error) {
	idx := 0
	idx, rowCount := utils.ParseInt16(message.Data, idx)

	values := make([][]byte, rowCount)
	for i := 0; i < rowCount; i++ {
		idx, valueLength := utils.ParseInt32(message.Data, idx)
		idx, value := utils.ParseBytes(message.Data, idx, valueLength)
		values[i] = value
	}

	return &DataRowMessage{values}, nil
}

func (m *DataRowMessage) Pack() []byte {
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
	nameBytes := []byte(d.Name)
	messageLength := d.byteLength()

	out := make([]byte, messageLength+1) // +1 for the kind of message
	idx := 0

	// Write the name of the field
	idx = utils.WriteCString(out, idx, nameBytes)
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
	idx = utils.WriteInt16(out, idx, d.format)

	return out
}

// ParameterStatusMessage represents the message sent by the server to inform the client of sever parameter values
type ParameterStatusMessage struct {
	Name  string
	Value string
}

func BuildParameterStatusMessage(name string, value string) *ParameterStatusMessage {
	return &ParameterStatusMessage{name, value}
}

func (m *ParameterStatusMessage) getByteLength() int {
	return len(m.Name) + 1 + len(m.Value) + 1
}

// Message interface implementation for ParameterStatusMessage
func (m *ParameterStatusMessage) Unpack(message *RawMessage) (*ParameterStatusMessage, error) {
	idx := 0
	idx, name, err := utils.ParseCString(message.Data, idx)
	if err != nil {
		return nil, err
	}
	idx, value, err := utils.ParseCString(message.Data, idx)
	if err != nil {
		return nil, err
	}

	return &ParameterStatusMessage{name, value}, nil
}

func (m *ParameterStatusMessage) Pack() []byte {
	messageLength := m.getByteLength() + 4 // content + length
	out := make([]byte, messageLength+1)   // +1 for the kind of message
	idx := 0

	idx = utils.WriteByte(out, idx, byte(BMESSAGE_PARAMETER_STATUS))
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteCString(out, idx, []byte(m.Name))
	idx = utils.WriteCString(out, idx, []byte(m.Value))

	return out
}

// BackendKeyDataMessage represents the message sent by the server to inform the client of the process id and secret key
type BackendKeyDataMessage struct {
	ProcessID int
	SecretKey int
}

func BuildBackendKeyDataMessage(processID int, secretKey int) *BackendKeyDataMessage {
	return &BackendKeyDataMessage{processID, secretKey}
}

// Message interface implementation for BackendKeyDataMessage
func (m *BackendKeyDataMessage) Unpack(message *RawMessage) (*BackendKeyDataMessage, error) {
	idx := 0
	idx, processID := utils.ParseInt32(message.Data, idx)
	idx, secretKey := utils.ParseInt32(message.Data, idx)

	return &BackendKeyDataMessage{processID, secretKey}, nil
}

func (m *BackendKeyDataMessage) Pack() []byte {
	messageLength := 4 + 4 + 4           // length + process id + secret key
	out := make([]byte, messageLength+1) // +1 for the kind of message (1 byte

	idx := 0

	idx = utils.WriteByte(out, idx, byte(BMESSAGE_BACKEND_KEY_DATA))
	idx = utils.WriteInt32(out, idx, messageLength)
	idx = utils.WriteInt32(out, idx, m.ProcessID)
	_ = utils.WriteInt32(out, idx, m.SecretKey)

	return out
}

// ReadyForQueryMessage represents the message sent by the server to indicate that the server is ready to accept a new query
type ReadyForQueryMessage struct {
	TransactionStatus byte
}

func BuildReadyForQueryMessage(status byte) *ReadyForQueryMessage {
	return &ReadyForQueryMessage{status}
}

// Message interface implementation for ReadyForQueryMessage
func (m *ReadyForQueryMessage) Unpack(message *RawMessage) (*ReadyForQueryMessage, error) {
	return &ReadyForQueryMessage{}, nil
}

func (m *ReadyForQueryMessage) Pack() []byte {
	messageLength := 4 + 1               // length + status
	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_READY_FOR_QUERY))
	idx = utils.WriteInt32(out, 1, messageLength)
	_ = utils.WriteByte(out, idx, m.TransactionStatus)

	return out
}

// NoDataMessage represents the message sent by the server to indicate that there is no data to return
type NoDataMessage struct{}

func BuildNoDataMessage() *NoDataMessage {
	return &NoDataMessage{}
}

// Message interface implementation for NoDataMessage
func (m *NoDataMessage) Unpack(message *RawMessage) (*NoDataMessage, error) {
	return &NoDataMessage{}, nil
}

func (m *NoDataMessage) Pack() []byte {
	messageLength := 4
	out := make([]byte, messageLength+1) // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte(BMESSAGE_NO_DATA))
	_ = utils.WriteInt32(out, idx, messageLength)

	return out
}

// CommandCompleteMessage represents the message sent by the server to indicate that a command has been completed
type CommandCompleteMessage struct {
	Command string
}

func BuildCommandCompleteMessage(command string) *CommandCompleteMessage {
	return &CommandCompleteMessage{command}
}

// Message interface implementation for CommandCompleteMessage
func (m *CommandCompleteMessage) Unpack(message *RawMessage) (*CommandCompleteMessage, error) {
	return &CommandCompleteMessage{string(message.Data)}, nil
}

func (m *CommandCompleteMessage) Pack() []byte {
	messageLength := len(m.Command) + 1 + 4 // content + length
	out := make([]byte, messageLength+1)    // +1 for the kind of message

	idx := 0
	idx = utils.WriteByte(out, idx, byte('C'))
	idx = utils.WriteInt32(out, idx, messageLength)
	_ = utils.WriteCString(out, idx, []byte(m.Command))

	return out
}
