package main

import (
	"net/http"

	log "github.com/sirupsen/logrus"
)

type App struct {
	logger log.FieldLogger
}

//Initialise can be moved to its own package and wraps up all the API level stuff
func Initialise() *App {
	a := &App{}
	a.configureLogging()
	return a
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
		"logLevel":    log.GetLevel(),
	})
}

func (a *App) Run() {
	a.logger.Info("Starting application")

	//Use a more sophiscated router like chi or something
	http.HandleFunc("/_system/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("Healthy"))
	})

	a.logger.Fatal(http.ListenAndServe(":8080", nil)) //Move port to config
}

func main() {
	app := Initialise()

	app.Run()
}
