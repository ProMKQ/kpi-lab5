package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

var testServersPool = []string{
	"localhost:8080",
	"localhost:8081",
	"localhost:8082",
}

const key = "dmwteam"

func main() {
	client := &http.Client{Timeout: 5 * time.Second}

	for _, server := range testServersPool {
		url := fmt.Sprintf("http://%s/api/v1/some-data?key=%s", server, key)
		resp, err := client.Get(url)
		if err != nil {
			log.Printf("[ERROR] %s → %s", server, err)
			continue
		}
		defer resp.Body.Close()

		log.Printf("=== Server %s ===", server)

		if resp.StatusCode == http.StatusOK {
			var data any
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				log.Printf("Invalid JSON: %v", err)
				continue
			}
			log.Printf("Data received: %v\n", data)
		} else if resp.StatusCode == http.StatusNotFound {
			log.Println("Key not found")
		} else {
			log.Printf(" Unexpected status: %d\n", resp.StatusCode)
		}
	}
}
