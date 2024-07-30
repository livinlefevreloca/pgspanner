package main

import (
	"io"
	"log"
	"log/slog"
	"os"
)

func getLogLevel(level string) slog.Level {
	switch level {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func ConfigureLogger(config LoggingConfig) {
	options := &slog.HandlerOptions{
		AddSource: true,
		Level:     getLogLevel(config.LogLevel),
	}

	var stream io.Writer
	var err error
	if config.LogFile == "" {
		stream = os.Stdout
	} else {
		stream, err = os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file: %s", err)
		}
	}

	var handler slog.Handler
	if config.Json {
		handler = slog.NewJSONHandler(stream, options)
	} else {
		handler = slog.NewTextHandler(stream, options)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
