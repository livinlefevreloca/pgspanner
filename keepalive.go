package main

import (
	"fmt"
	"log/slog"
	"time"
)

func runWithRecovery(componentName string, f func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Component paniced, but recovery was posible", "Error", r, "Component", componentName)
		}
	}()
	f()
}

func StartComponentWithKeepAlive(
	name string,
	component func(config *SpannerConfig, keepAlive *KeepAlive, connectionReqester *ConnectionRequester),
	timeout time.Duration,
	config *SpannerConfig,
	connectionReqester *ConnectionRequester,
	disabled bool,
) *KeepAlive {
	keepAlive := NewKeepAlive(
		name,
		component,
		timeout,
		disabled,
	)
	if disabled {
		return keepAlive
	}

	slog.Info("Starting component with KeepAlive", "Name", name)

	go runWithRecovery(name, func() {
		component(config, keepAlive, connectionReqester)
	})

	return keepAlive
}

func RunKeepAliveHandler(
	config *SpannerConfig,
	keepAlives []*KeepAlive,
	connectionRequester *ConnectionRequester,
) {
	// Start the client keep alive handler
	maxTimeout := GetMaxKeepAliveTimeout(keepAlives)
	for {
		slog.Debug("Running KeepAlive Handler loop", "Sleep", maxTimeout)
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
	name     string
	channel  chan bool
	timeout  time.Duration
	f        func(*SpannerConfig, *KeepAlive, *ConnectionRequester)
	disabled bool
}

func NewKeepAlive(name string, f func(*SpannerConfig, *KeepAlive, *ConnectionRequester), timeout time.Duration, disabled bool) *KeepAlive {
	return &KeepAlive{
		name:     name,
		channel:  make(chan bool, 10),
		timeout:  timeout,
		f:        f,
		disabled: disabled,
	}
}

func (k *KeepAlive) expect(config *SpannerConfig, connectionRequester *ConnectionRequester) {
	time.Sleep(k.timeout)
	slog.Debug("Check for liveness", "Component", k.name)
	select {
	case <-k.channel:
		slog.Debug("Component Channel has messages", "Count", len(k.channel))
		k.Clear()
		slog.Debug("Component Channel has messages after clearing", "Count", len(k.channel))
	default:
		slog.Warn(fmt.Sprintf("Component: %s is not alive. Restarting...", k.name))
		go k.restart(config, connectionRequester)
	}
}

func (k *KeepAlive) Notify() {
	if k.disabled {
		return
	}
	slog.Debug("Queue before notifying", "Count", len(k.channel), "Component", k.name)
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
	slog.Debug("Queue after clearing", "Count", len(k.channel), "Component", k.name)
}

func (k *KeepAlive) restart(config *SpannerConfig, connectionReqester *ConnectionRequester) {
	runWithRecovery(k.name, func() {
		k.f(config, k, connectionReqester)
	})
}
