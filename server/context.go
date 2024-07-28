package server

import "github.com/livinlefevreloca/pgspanner/config"

type ServerProcessIdentity struct {
	BackendPid   int
	BackendKey   int
	DatabaseName string
	ClusterHost  string
	ClusterPort  int
}

type serverConnectionContext struct {
	Parmeters      map[string]string
	ServerIdentity ServerProcessIdentity
	Database       *config.DatabaseConfig
	Cluster        *config.ClusterConfig
}

func newServerConnectionContext(
	clusterConfig *config.ClusterConfig,
	databaseConfig *config.DatabaseConfig,

) *serverConnectionContext {
	return &serverConnectionContext{
		Parmeters: make(map[string]string),
		Database:  databaseConfig,
		Cluster:   clusterConfig,
	}
}

func (s *serverConnectionContext) GetParameter(key string) string {
	return s.Parmeters[key]
}

func (s *serverConnectionContext) SetParameter(key string, value string) {
	s.Parmeters[key] = value
}
