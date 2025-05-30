package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	datastore "github.com/ProMKQ/kpi-lab5/database"
)

var db *datastore.Db

func main() {
	var err error
	db, err = datastore.Open("db-data") // збереження файлів у папці db-data
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	http.HandleFunc("/db/", handleRequest)

	fmt.Println("DB service listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("type") == "int64" {
			val, err := db.GetInt64(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"key": key, "value": val})
		} else {
			val, err := db.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"key": key, "value": val})
		}
	case http.MethodPost:
		var req map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		value, ok := req["value"]
		if !ok {
			http.Error(w, "missing value", http.StatusBadRequest)
			return
		}

		var err error
		switch v := value.(type) {
		case float64:
			err = db.PutInt64(key, int64(v))
		case string:
			err = db.Put(key, v)
		default:
			err = fmt.Errorf("unsupported value type")
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	default:
		http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
	}
}
