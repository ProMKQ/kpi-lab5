package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ProMKQ/kpi-lab4/httptools"
	"github.com/ProMKQ/kpi-lab4/signal"
)

var (
	port         = flag.Int("port", 8090, "load balancer port")
	timeoutSec   = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https        = flag.Bool("https", false, "whether backends support HTTPs")
	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var timeout time.Duration

type Server struct {
	Address string
	ConnCnt int
	Healthy bool
	mu      sync.Mutex
}

var serversPool = []*Server{
	{Address: "server1:8080"},
	{Address: "server2:8080"},
	{Address: "server3:8080"},
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func getLeastConnServer() *Server {
	var selected *Server
	for _, s := range serversPool {
		s.mu.Lock()
		if s.Healthy {
			if selected == nil || s.ConnCnt < selected.ConnCnt {
				selected = s
			}
		}
		s.mu.Unlock()
	}
	return selected
}

func main() {
	flag.Parse()
	timeout = time.Duration(*timeoutSec) * time.Second

	// Start health check goroutines
	for _, server := range serversPool {
		s := server
		go func() {
			for range time.Tick(5 * time.Second) {
				isHealthy := health(s.Address)
				s.mu.Lock()
				s.Healthy = isHealthy
				s.mu.Unlock()
				log.Println(s.Address, "healthy:", isHealthy)
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server := getLeastConnServer()
		if server == nil {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		server.mu.Lock()
		server.ConnCnt++
		server.mu.Unlock()

		defer func() {
			server.mu.Lock()
			server.ConnCnt--
			server.mu.Unlock()
		}()

		forward(server.Address, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
