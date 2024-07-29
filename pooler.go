package main

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/livinlefevreloca/pgspanner/utils"
)

const (
	CONNECTION_SWEEP_INTERVAL = 5 * time.Second
)

type Pooler struct {
	connections    []*ServerConnection
	clusterConfig  *ClusterConfig
	databaseConfig *DatabaseConfig
}

func newPooler(
	databaseConfig *DatabaseConfig,
	clusterConfig *ClusterConfig,
) *Pooler {
	conections := make([]*ServerConnection, 0, databaseConfig.PoolSettings.MaxOpenConns)
	return &Pooler{
		connections:    conections,
		databaseConfig: databaseConfig,
		clusterConfig:  clusterConfig,
	}
}

func (p *Pooler) getPoolSettings() *PoolConfig {
	return &p.databaseConfig.PoolSettings
}

func (p *Pooler) getAddr() string {
	return fmt.Sprintf("%s:%d", p.clusterConfig.Host, p.clusterConfig.Port)
}

func (p *Pooler) removeConnection(connection *ServerConnection) {
	if len(p.connections) == 0 {
		return
	}
	var index int
	for i, conn := range p.connections {
		if conn.GetBackendPid() == connection.GetBackendPid() {
			index = i
			break
		}
	}
	slices.Delete(p.connections, index, index)
}

func (p *Pooler) getConnection(frontendPid int) (*ServerConnection, error) {
	connectionCount := len(p.connections)
	poolSettings := p.getPoolSettings()
	var connection *ServerConnection
	var err error
	for {
		if connectionCount == 0 {
			connection, err = CreateServerConnection(p.databaseConfig, p.clusterConfig)
			if err != nil {
				slog.Error(
					"Error creating connection",
					"Pooler", p.clusterConfig.Name,
					"Error", err,
				)
				return nil, err
			}
			break
		}

		var ptr **ServerConnection
		p.connections, ptr = utils.Pop(p.connections)
		connection = *ptr

		if connection.IsPoisoned() {
			connection.Close()
			connectionCount = len(p.connections)
		} else if connection.GetAge() > int64(poolSettings.MaxConnLifetime) {
			slog.Info(
				"Closing connection. Connection has exceeded max lifetime",
				"Pooler", p.getAddr(),
				"BackendPid", connection.GetBackendPid(),
			)
			connection.Close()
			connectionCount = len(p.connections)
		} else {
			break
		}
	}

	return connection, nil
}

func (p *Pooler) returnConnection(connection ServerConnection, frontendPid int) {
	poolSettings := p.getPoolSettings()
	if len(p.connections) < poolSettings.MaxOpenConns {
		slog.Info(
			"Returning connection",
			"Pooler", p.getAddr(),
			"BackendPid", connection.GetBackendPid(),
		)
		p.connections = append(p.connections, &connection)
	} else if connection.GetAge() > int64(poolSettings.MaxConnLifetime) {
		slog.Info(
			"Closing connection. Connection has exceeded max lifetime",
			"Pooler", p.getAddr(),
			"BackendPid", connection.GetBackendPid(),
		)
		connection.Close()
	} else {
		slog.Info(
			"Closing connection. Pool is full",
			"Pooler", p.getAddr(),
			"BackendPid", connection.GetBackendPid(),
		)
		connection.Close()
	}
}

func (p *Pooler) CloseConnection(connection *ServerConnection, frontendPid int) {
	connection.Close()
}

type PoolerManager struct {
	poolers          map[string]map[string]*Pooler
	ConnectionServer *ConnectionRequester
	connectionTable  map[int][]ServerProcessIdentity
}

func NewPoolerManager(config *SpannerConfig, server *ConnectionRequester) *PoolerManager {
	poolers := make(map[string]map[string]*Pooler)
	for _, database := range config.Databases {
		for _, cluster := range database.Clusters {
			if poolers[database.Name] == nil {
				poolers[database.Name] = make(map[string]*Pooler)
			}
			poolers[database.Name][cluster.Name] = newPooler(&database, &cluster)
		}
	}
	return &PoolerManager{
		poolers:          poolers,
		ConnectionServer: server,
	}
}

func (pm *PoolerManager) SendConnection(request ConnectionRequest) {
	pooler := pm.poolers[request.database][request.cluster]
	var response ConnectionResponse
	connection, err := pooler.getConnection(request.FrontendPid)
	if err != nil {
		response = ConnectionResponse{
			Event:  ACTION_GET_CONNECTION,
			Result: RESULT_ERROR,
			Detail: err,
			Conn:   nil,
		}
	} else {
		response = ConnectionResponse{
			Event: ACTION_GET_CONNECTION,
			Conn:  connection,
		}
	}
	if pm.connectionTable == nil {
		pm.connectionTable = make(map[int][]ServerProcessIdentity)
	}
	if pm.connectionTable[request.FrontendPid] == nil {
		pm.connectionTable[request.FrontendPid] = make([]ServerProcessIdentity, 0)
	}
	pm.connectionTable[request.FrontendPid] = append(pm.connectionTable[request.FrontendPid], connection.GetServerIdentity())
	request.responder <- response
}

func (pm *PoolerManager) CloseConnection(request ConnectionRequest) {
	pooler := pm.poolers[request.database][request.cluster]
	if request.Connection == nil {
		slog.Error(
			"Received nil connection in CloseConnection",
			"cluster", request.cluster,
			"database", request.database,
		)
		return
	}
	if pm.connectionTable == nil {
		pm.connectionTable = make(map[int][]ServerProcessIdentity)
	}
	if pm.connectionTable[request.FrontendPid] == nil {
		pm.connectionTable[request.FrontendPid] = make([]ServerProcessIdentity, 0)
	}
	pm.connectionTable[request.FrontendPid] = utils.DeleteFromUnsorted[ServerProcessIdentity](
		pm.connectionTable[request.FrontendPid],
		request.Connection.GetServerIdentity(),
	)
	pooler.CloseConnection(request.Connection, request.FrontendPid)
}

func (pm *PoolerManager) ReturnConnection(request ConnectionRequest) {
	pooler := pm.poolers[request.database][request.cluster]
	if request.Connection == nil {
		slog.Error(
			"Received nil connection in ReturnConnection",
			"cluster", request.cluster,
			"database", request.database,
		)
		return
	}
	if pm.connectionTable == nil {
		pm.connectionTable = make(map[int][]ServerProcessIdentity)
	}
	if pm.connectionTable[request.FrontendPid] == nil {
		pm.connectionTable[request.FrontendPid] = make([]ServerProcessIdentity, 0)
	}
	pm.connectionTable[request.FrontendPid] = utils.DeleteFromUnsorted[ServerProcessIdentity](
		pm.connectionTable[request.FrontendPid],
		request.Connection.GetServerIdentity(),
	)
	pooler.returnConnection(*request.Connection, request.FrontendPid)
}

type ConnectionMappingNotFound struct {
	FrontendPid int
}

func (e ConnectionMappingNotFound) Error() string {
	return fmt.Sprintf("No connections found for FrontendPid: %d", e.FrontendPid)
}

func (pm *PoolerManager) SendConnectionMapping(request ConnectionRequest) {
	if pm.connectionTable == nil {
		pm.connectionTable = make(map[int][]ServerProcessIdentity)
	}

	var serverIdentities []ServerProcessIdentity
	var result string
	serverIdentities, ok := pm.connectionTable[request.FrontendPid]
	if !ok {
		serverIdentities = make([]ServerProcessIdentity, 0)
		result = RESULT_ERROR
	} else {
		result = RESULT_SUCCESS
	}

	response := ConnectionResponse{
		Event:       ACTION_GET_CONNECTION_MAPPING,
		Result:      result,
		Detail:      ConnectionMappingNotFound{FrontendPid: request.FrontendPid},
		ConnMapping: serverIdentities,
	}
	request.responder <- response
}

func RunPoolManager(config *SpannerConfig, keepAlive *KeepAlive, connectionReqester *ConnectionRequester) {
	// Start the pool manager
	poolManager := NewPoolerManager(config, connectionReqester)
	timeout := time.After(CONNECTION_SWEEP_INTERVAL)
	for {
		select {
		case request := <-connectionReqester.ReceiveConnectionRequest():
			slog.Info("Received connection request", "action", request.Event)
			switch request.Event {
			case ACTION_GET_CONNECTION:
				poolManager.SendConnection(*request)
			case ACTION_RETURN_CONNECTION:
				poolManager.ReturnConnection(*request)
			case ACTION_CLOSE_CONNECTION:
				poolManager.CloseConnection(*request)
			case ACTION_GET_CONNECTION_MAPPING:
				poolManager.SendConnectionMapping(*request)
			}
		case <-timeout:
			keepAlive.Notify()
			timeout = time.After(CONNECTION_SWEEP_INTERVAL)
		}
	}
}
