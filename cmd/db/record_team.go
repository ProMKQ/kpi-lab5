package main

import (
	"fmt"
	"github.com/ProMKQ/kpi-lab5/datastore"
	"log"
	"time"
)

func main() {
	db, err := datastore.Open("db-data")
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	key := "dmwteam"
	value := time.Now().Format("2006-01-02")

	if err := db.Put(key, value); err != nil {
		log.Fatalf("failed to write to db: %v", err)
	}

	fmt.Printf("Team %s saved with value %s\n", key, value)
}
