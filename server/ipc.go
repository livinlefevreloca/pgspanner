package server

const (
	ACTION_GET_CONNECTION    = "GET_CONNECTION"
	ACTION_RETURN_CONNECTION = "RETURN_CONNECTION"
	ACTION_CLOSE_CONNECTION  = "CLOSE_CONNECTION"
)

const (
	RESULT_SUCCESS = "SUCCESS"
	RESULT_ERROR   = "ERROR"
)

type ConnectionRequest struct {
	Event      string
	database   string
	cluster    string
	Connection *ServerConnection
	responder  chan ConnectionResponse
}

type ConnectionResponse struct {
	Event  string
	Result string
	Detail error
	Conn   *ServerConnection
}

type ConnectionRequester struct {
	channel chan *ConnectionRequest
}

func NewConnectionRequester() *ConnectionRequester {
	// Use a buffered channel to avoid blocking the requester
	return &ConnectionRequester{channel: make(chan *ConnectionRequest, 2)}
}

func (cr *ConnectionRequester) ReceiveConnectionRequest() chan *ConnectionRequest {
	return cr.channel
}

func (cr *ConnectionRequester) RequestConnection(database string, cluster string) ConnectionResponse {
	response := make(chan ConnectionResponse)
	request := ConnectionRequest{Event: ACTION_GET_CONNECTION, database: database, cluster: cluster, responder: response}
	cr.channel <- &request
	return <-response
}

func (cr *ConnectionRequester) ReturnConnection(conn *ServerConnection, database string, cluster string) {
	request := ConnectionRequest{Event: ACTION_RETURN_CONNECTION, Connection: conn, database: database, cluster: cluster}
	cr.channel <- &request
}
