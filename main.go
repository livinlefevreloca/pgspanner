package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	TIMEOUT = 10 * time.Second
)

func main() {
	// Read the config
	configPath := flag.String("config", "config.toml", "Path to the config file")
	pidFile := flag.String("pidfile", "spanner.pid", "Path to the pid file")
	noKeepAlive := flag.Bool("nokeepalive", false, "Enable keep alive")
	flag.Parse()

	fmt.Println("Keep alive is off:", *noKeepAlive)

	var config SpannerConfig
	_, err := toml.DecodeFile(*configPath, &config)
	if err != nil {
		log.Fatal("Error reading config file", err)
	}

	if config.PidFile != "" {
		// Write the pid file
		os.WriteFile(*pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
	}
	ConfigureLogger(config.Logging)

	connRequester := NewConnectionRequester()

	chKeepAlive := StartComponentWithKeepAlive(
		"clientConnectionHandler",
		clientConnectionHandler,
		TIMEOUT*2,
		&config,
		connRequester,
		*noKeepAlive,
	)
	poolKeepAlive := StartComponentWithKeepAlive(
		"poolManager",
		RunPoolManager,
		CONNECTION_SWEEP_INTERVAL*4,
		&config,
		connRequester,
		*noKeepAlive,
	)
	if *noKeepAlive {
		go RunPoolManager(&config, chKeepAlive, connRequester)
		clientConnectionHandler(&config, poolKeepAlive, connRequester)
	} else {
		var keepAlives []*KeepAlive
		keepAlives = append(keepAlives, chKeepAlive)
		keepAlives = append(keepAlives, poolKeepAlive)
		RunKeepAliveHandler(&config, keepAlives, connRequester)
	}
}
