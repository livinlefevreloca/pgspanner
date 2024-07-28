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
	"github.com/livinlefevreloca/pgspanner/utils"
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
	notifier := make(chan bool, 10)
	go poolManager.StartConnectionSweeper(&notifier)
	for {
		slog.Info("Pool manager loop")
		time.AfterFunc(server.CONNECTION_SWEEP_INTERVAL*2, func() {
			keepAlive.Notify()
			select {
			case <-notifier:
				utils.ClearChannel(notifier)
			default:
				slog.Warn("Connection sweeper is not running. Restarting...")
				go poolManager.StartConnectionSweeper(&notifier)
			}
		})
		select {
		case request := <-connectionReqester.ReceiveConnectionRequest():
			slog.Info("Received connection request", "action", request.Event)
			switch request.Event {
			case server.ACTION_GET_CONNECTION:
				poolManager.SendConnectionResponse(*request)
			case server.ACTION_RETURN_CONNECTION:
				poolManager.ReturnConnection(*request)
				slog.Info("Returned connection to pool")
			}
		case <-time.After(server.CONNECTION_SWEEP_INTERVAL * 4):
		}
	}
}

func clientConnectionHandler(config *config.SpannerConfig, keepAlive *keepalive.KeepAlive, connectionReqester *server.ConnectionRequester) {
	slog.Info("Client connection handler started")
	addr := net.TCPAddr{
		IP:   net.ParseIP("0.0.0.0"),
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
			slog.Info("Client connection handler loop timeout")
			continue
		} else if err != nil {
			log.Fatal(err)
			return
		}
		slog.Info("Client connected. Starting connection loop...")
		go client.ConnectionLoop(conn, config, connectionReqester)
		keepAlive.Notify()
		slog.Info("Client connection handler loop")
	}
}
