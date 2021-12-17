package main

import (
	log "github.com/sirupsen/logrus"
)

type App struct {
	logger log.FieldLogger
}

func (a *App) configureLogging() {
	log.SetLevel(log.TraceLevel)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})

	a.logger = log.WithFields(log.Fields{
		"application": "go-sqs-consumer",
		"version":     "0.0.0",
		"env":         "local",
	})
}

func main() {
	app := App{}

	app.configureLogging()
	app.logger.Info("Starting application")
}
