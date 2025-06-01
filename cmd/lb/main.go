package main

import (
	"log"
	"net/http"

	"github.com/ProMKQ/kpi-lab5/httptools"
	"github.com/ProMKQ/kpi-lab5/signal"
)

func main() {
	h := http.NewServeMux()

	h.HandleFunc("/api/v1/some-data", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Balancer received request:", r.URL.String())
		http.Error(w, "Not implemented", http.StatusNotImplemented)
	})

	server := httptools.CreateServer(8090, h)
	server.Start()

	signal.WaitForTerminationSignal()
}
