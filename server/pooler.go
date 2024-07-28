package server

import (
	"log/slog"
	"time"

	"github.com/livinlefevreloca/pgspanner/config"
)

const (
	CONNECTION_SWEEP_INTERVAL = 5 * time.Second
)

type Pooler struct {
	connections   []ServerConnection
	clusterConfig *config.ClusterConfig
	poolConfig    *config.PoolConfig
}

func newPooler(
	clusterConfig *config.ClusterConfig,
	poolConfig *config.PoolConfig,
) *Pooler {
	conections := make([]ServerConnection, 0, poolConfig.MaxOpenConns)
	return &Pooler{
		connections:   conections,
		clusterConfig: clusterConfig,
		poolConfig:    poolConfig,
	}
}

func (p *Pooler) getConnection() (*ServerConnection, error) {
	connectionCount := len(p.connections)
	var connection *ServerConnection
	var err error
	if connectionCount == 0 {
		connection, err = CreateServerConnection(p.clusterConfig)
		if err != nil {
			slog.Error(
				"Error creating connection",
				"Pooler", p.clusterConfig.Name,
				"Host", p.clusterConfig.Host,
				"Port", p.clusterConfig.Port,
				"Error", err,
			)
			return nil, err
		}
	} else {
		connection = &p.connections[connectionCount-1]
		p.connections = p.connections[:connectionCount-1]
	}
	return connection, nil
}

func (p *Pooler) returnConnection(connection ServerConnection) {
	if len(p.connections) < p.poolConfig.MaxOpenConns {
		p.connections = append(p.connections, connection)
	} else {
		slog.Info(
			"Closing connection. Pool is full",
			"Pooler", p.clusterConfig.Name,
			"BackendPid", connection.GetBackendPid(),
		)
		connection.Close()
	}
}

type PoolerManager struct {
	poolers          map[string]map[string]*Pooler
	ConnectionServer *ConnectionRequester
}

func NewPoolerManager(config *config.SpannerConfig, server *ConnectionRequester) *PoolerManager {
	poolers := make(map[string]map[string]*Pooler)
	for _, database := range config.Databases {
		for _, cluster := range database.Clusters {
			if poolers[database.Name] == nil {
				poolers[database.Name] = make(map[string]*Pooler)
			}
			poolers[database.Name][cluster.Name] = newPooler(&cluster, &database.PoolSettings)
		}
	}
	return &PoolerManager{
		poolers:          poolers,
		ConnectionServer: server,
	}
}

func (pm *PoolerManager) SendConnectionResponse(request ConnectionRequest) {
	pooler := pm.poolers[request.database][request.cluster]
	var response ConnectionResponse
	connection, err := pooler.getConnection()
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
	request.responder <- response
	slog.Info(
		"Sent connection response",
		"cluster", request.cluster,
		"database", request.database,
	)
}

func (pm *PoolerManager) ReturnConnection(request ConnectionRequest) {
	pooler := pm.poolers[request.database][request.cluster]
	pooler.returnConnection(*request.Connection)
	slog.Info(
		"Returned connection",
		"cluster", request.cluster,
		"database", request.database,
	)
}

func (pm *PoolerManager) StartConnectionSweeper(notifier *chan bool) {
	for {
		for _, database := range pm.poolers {
			for _, pooler := range database {
				for _, connection := range pooler.connections {
					age := connection.GetAge()
					if age > int64(pooler.poolConfig.MaxConnLifetime) {
						slog.Info(
							"Closing connection due to age",
							"pid", connection.GetBackendPid(),
							"Pooler", pooler.clusterConfig.Name,
							"Age", age,
						)
						connection.Close()
					}
				}
			}
		}
		*notifier <- true
		time.Sleep(CONNECTION_SWEEP_INTERVAL)
	}
}
