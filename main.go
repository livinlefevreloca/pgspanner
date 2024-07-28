package main

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/livinlefevreloca/pgspanner/client"
	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/keepalive"
	"github.com/livinlefevreloca/pgspanner/server"
)

const (
	TIMEOUT = 10 * time.Second
)

func main() {
	// Read the config
	var config config.SpannerConfig
	_, err := toml.DecodeFile("config.toml", &config)
	if err != nil {
		log.Fatal("Error reading config file", err)
	}
	fmt.Println(config.Display())

	connRequester := server.NewConnectionRequester()

	var keepAlives []*keepalive.KeepAlive
	chKeepAlive := keepalive.StartComponentWithKeepAlive(
		"clientConnectionHandler",
		clientConnectionHandler,
		TIMEOUT*2,
		&config,
		connRequester,
	)
	keepAlives = append(keepAlives, chKeepAlive)
	poolKeepAlive := keepalive.StartComponentWithKeepAlive(
		"poolManager",
		RunPoolManager,
		server.CONNECTION_SWEEP_INTERVAL*4,
		&config,
		connRequester,
	)
	keepAlives = append(keepAlives, poolKeepAlive)

	keepalive.RunKeepAliveHandler(&config, keepAlives, connRequester)
}

func RunPoolManager(config *config.SpannerConfig, keepAlive *keepalive.KeepAlive, connectionReqester *server.ConnectionRequester) {
	// Start the pool manager
	poolManager := server.NewPoolerManager(config, connectionReqester)
	for {
		select {
		case request := <-connectionReqester.ReceiveConnectionRequest():
			slog.Info("Received connection request", "action", request.Event)
			switch request.Event {
			case server.ACTION_GET_CONNECTION:
				poolManager.SendConnection(*request)
			case server.ACTION_RETURN_CONNECTION:
				poolManager.ReturnConnection(*request)
			case server.ACTION_CLOSE_CONNECTION:
				poolManager.CloseConnection(*request)
			case server.ACTION_GET_CONNECTION_MAPPING:
				poolManager.SendConnectionMapping(*request)
			}
		case <-time.After(server.CONNECTION_SWEEP_INTERVAL):
			keepAlive.Notify()
		}
	}
}

func clientConnectionHandler(config *config.SpannerConfig, keepAlive *keepalive.KeepAlive, connectionReqester *server.ConnectionRequester) {
	// Start the client pid counter
	clientPid := 1
	slog.Info("Client connection handler started")
	if config.ListenAddr == "localhost" || config.ListenAddr == "" {
		config.ListenAddr = "127.0.0.1"
	}
	addr := net.TCPAddr{
		IP:   net.ParseIP(config.ListenAddr),
		Port: config.ListenPort,
		Zone: "",
	}

	l, err := net.ListenTCP("tcp", &addr)
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Listening on port 8000")
	defer l.Close()
	for {
		l.SetDeadline(time.Now().Add(TIMEOUT))
		conn, err := l.Accept()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			keepAlive.Notify()
			slog.Debug("Client connection handler loop timeout")
			continue
		} else if err != nil {
			log.Fatal(err)
			return
		}
		slog.Info("Client connected. Starting connection loop...")
		go client.ConnectionLoop(conn, config, connectionReqester, clientPid)
		keepAlive.Notify()
		slog.Debug("Client connection handler loop")
	}
}
