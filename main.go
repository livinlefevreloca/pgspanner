package main

import (
	"fmt"
	"log"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	TIMEOUT = 10 * time.Second
)

func main() {
	// Read the config
	var config SpannerConfig
	_, err := toml.DecodeFile("config.toml", &config)
	if err != nil {
		log.Fatal("Error reading config file", err)
	}
	fmt.Println(config.Display())

	connRequester := NewConnectionRequester()

	var keepAlives []*KeepAlive
	chKeepAlive := StartComponentWithKeepAlive(
		"clientConnectionHandler",
		clientConnectionHandler,
		TIMEOUT*2,
		&config,
		connRequester,
	)
	keepAlives = append(keepAlives, chKeepAlive)
	poolKeepAlive := StartComponentWithKeepAlive(
		"poolManager",
		RunPoolManager,
		CONNECTION_SWEEP_INTERVAL*4,
		&config,
		connRequester,
	)
	keepAlives = append(keepAlives, poolKeepAlive)

	RunKeepAliveHandler(&config, keepAlives, connRequester)
}
