package keepalive

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/livinlefevreloca/pgspanner/config"
	"github.com/livinlefevreloca/pgspanner/server"
	"github.com/livinlefevreloca/pgspanner/utils"
)

func StartComponentWithKeepAlive(
	name string,
	component func(config *config.SpannerConfig, keepAlive *KeepAlive, connectionReqester *server.ConnectionRequester),
	timeout time.Duration,
	config *config.SpannerConfig,
	connectionReqester *server.ConnectionRequester,
) *KeepAlive {
	keepAlive := NewKeepAlive(
		name,
		component,
		timeout,
	)

	slog.Info("Starting component with KeepAlive", "Name", name)
	go component(config, keepAlive, connectionReqester)

	return keepAlive
}

func RunKeepAliveHandler(
	config *config.SpannerConfig,
	keepAlives []*KeepAlive,
	connectionRequester *server.ConnectionRequester,
) {
	// Start the client keep alive handler
	maxTimeout := GetMaxKeepAliveTimeout(keepAlives)
	for {
		slog.Info("Running KeepAlive Handler loop", "Sleep", maxTimeout)
		for _, keepAlive := range keepAlives {
			go keepAlive.expect(config, connectionRequester)
		}
		time.Sleep(maxTimeout)
	}
}

func GetMaxKeepAliveTimeout(keepAlives []*KeepAlive) time.Duration {
	var maxTimeout time.Duration
	for _, k := range keepAlives {
		if k.timeout > maxTimeout {
			maxTimeout = k.timeout
		}
	}
	return maxTimeout
}

type KeepAlive struct {
	name    string
	channel chan bool
	timeout time.Duration
	f       func(*config.SpannerConfig, *KeepAlive, *server.ConnectionRequester)
}

func NewKeepAlive(name string, f func(*config.SpannerConfig, *KeepAlive, *server.ConnectionRequester), timeout time.Duration) *KeepAlive {
	return &KeepAlive{
		name:    name,
		channel: make(chan bool, 10),
		timeout: timeout,
		f:       f,
	}
}

func (k *KeepAlive) expect(config *config.SpannerConfig, connectionRequester *server.ConnectionRequester) {
	time.Sleep(k.timeout)
	slog.Info("Check for liveness", "Component", k.name)
	select {
	case <-k.channel:
		slog.Info("Component Channel has messages", "Count", len(k.channel))
		if len(k.channel) > 0 {
			utils.ClearChannel(k.channel)
		}
		slog.Info("Component Channel has messages after clearing", "Count", len(k.channel))
	default:
		slog.Warn(fmt.Sprintf("Component: %s is not alive. Restarting...", k.name))
		go k.restart(config, connectionRequester)
	}
}

func (k *KeepAlive) Notify() {
	slog.Info("Queue before notifying", "Count", len(k.channel), "Component", k.name)
	k.Clear()
	k.channel <- true
}

func (k *KeepAlive) Clear() {
	for len(k.channel) > 0 {
		select {
		case <-k.channel:
		default:
		}
	}
	slog.Info("Queue after clearing", "Count", len(k.channel), "Component", k.name)
}

func (k *KeepAlive) restart(config *config.SpannerConfig, connectionReqester *server.ConnectionRequester) {
	k.f(config, k, connectionReqester)
}
