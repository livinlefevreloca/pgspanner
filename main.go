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
)

const (
	TIMEOUT = 2 * time.Second
)

type KeepAlive struct {
	name    string
	channel chan bool
	f       func(*config.SpannerConfig, *KeepAlive)
}

func (k *KeepAlive) notify() {
	k.channel <- true
}

func startClientConnectionHandler(config *config.SpannerConfig) KeepAlive {
	chKeepAliveChan := make(chan bool)

	chKeepAlive := KeepAlive{
		name:    "clientConnectionHandler",
		channel: chKeepAliveChan,
		f:       clientConnectionHandler,
	}

	// Start the client connection handler
	slog.Info("Starting client connection handler")
	go clientConnectionHandler(config, &chKeepAlive)

	return chKeepAlive
}

func RunKeepAliveHandler(config *config.SpannerConfig, keepAlives []KeepAlive) {
	// Start the client keep alive handler
	for {
		// If we havent received a message in 2 * TIMEOUT,
		time.Sleep(TIMEOUT * 2)
		for _, keepAlive := range keepAlives {
			select {
			case <-keepAlive.channel:
				continue
			default:
				slog.Warn(fmt.Sprintf("Component: %s is not alive. Restarting...", keepAlive.name))
				go keepAlive.f(config, &keepAlive)
			}
		}
	}
}

func main() {
	// Read the config
	var config config.SpannerConfig
	_, err := toml.DecodeFile("config.toml", &config)
	if err != nil {
		log.Fatal("Error reading config file", err)
	}
	fmt.Println(config.Display())

	var keepAlives []KeepAlive
	chKeepAlive := startClientConnectionHandler(&config)
	keepAlives = append(keepAlives, chKeepAlive)

	RunKeepAliveHandler(&config, keepAlives)
}

func clientConnectionHandler(config *config.SpannerConfig, keepAlive *KeepAlive) {
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
			keepAlive.notify()
			continue
		} else if err != nil {
			log.Fatal(err)
			return
		}
		slog.Info("Client connected. Starting connection loop...")
		go client.ConnectionLoop(conn, &config.Databases[0])
		keepAlive.notify()
	}
}
