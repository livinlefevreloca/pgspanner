package main

import "fmt"

type ClusterConfig struct {
	Name        string
	Host        string
	Port        int
	User        string
	PasswordEnv string
}

func (c *ClusterConfig) display() string {
	return "Cluster: " + c.Name + " Host: " + c.Host + " Port: " + fmt.Sprint(c.Port) + " User: " + c.User + " PasswordEnv: " + c.PasswordEnv
}

func (c *ClusterConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type PoolConfig struct {
	MaxOpenConns     int
	MaxIdleConns     int
	MaxConnLifetime  int
	IdleConnLifetime int
}

func (p *PoolConfig) display() string {
	return "MaxOpenConns: " + fmt.Sprint(p.MaxOpenConns) + " MaxIdleConns: " + fmt.Sprint(p.MaxIdleConns) + " MaxConnLifetime: " + fmt.Sprint(p.MaxConnLifetime) + " IdleConnLifetime: " + fmt.Sprint(p.IdleConnLifetime)
}

type DatabaseConfig struct {
	Name         string
	Clusters     []ClusterConfig
	AuthMethod   string
	SSL          bool
	ShouldPool   bool
	PoolSettings PoolConfig
}

func (d *DatabaseConfig) display() string {
	confStr := ""
	confStr += "Database: " + d.Name + "\n"
	confStr += "[[ Clusters ]]\n"
	for _, c := range d.Clusters {
		confStr += c.display() + "\n"
	}
	confStr += "AuthMethod: " + d.AuthMethod + "\n"
	confStr += "SSL: " + fmt.Sprint(d.SSL) + "\n"
	confStr += "ShouldPool: " + fmt.Sprint(d.ShouldPool) + "\n"
	confStr += "[[ PoolSettings ]]\n"
	confStr += d.PoolSettings.display() + "\n"
	return confStr
}

func (d *DatabaseConfig) GetClusterConfigByHostPort(addr string) (*ClusterConfig, bool) {
	for _, c := range d.Clusters {
		if c.GetAddr() == addr {
			return &c, true
		}
	}
	return nil, false
}

type LoggingConfig struct {
	LogLevel string
	LogFile  string
	Json     bool
}

func (l *LoggingConfig) display() string {
	return "LogLevel: " + l.LogLevel + " LogFile: " + l.LogFile + " Json: " + fmt.Sprint(l.Json)
}

type SpannerConfig struct {
	// Logging Config
	Logging LoggingConfig

	PidFile string

	// Frontend Config
	ListenPort int
	ListenAddr string

	// Backend Config
	Databases []DatabaseConfig
}

func (c *SpannerConfig) GetDatabaseConfigByName(name string) (*DatabaseConfig, bool) {
	for _, d := range c.Databases {
		if d.Name == name {
			return &d, true
		}
	}
	return nil, false
}

func (s *SpannerConfig) Display() string {
	confStr := ""
	confStr += s.Logging.display() + "\n"
	confStr += "ListenAddr: " + s.ListenAddr + "\n"
	confStr += "ListenPort: " + fmt.Sprint(s.ListenPort) + "\n"
	confStr += "[[ Databases ]]\n\n"
	for _, d := range s.Databases {
		confStr += d.display() + "\n"
	}
	return confStr
}
