package integration

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 5 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	const requests = 20
	responses := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/data", baseAddress))
			require.NoError(t, err)
			require.NotNil(t, resp)

			from := resp.Header.Get("lb-from")
			mu.Lock()
			responses[from]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	assert.True(t, len(responses) == 3, "expected requests to be distributed across 3 servers, got %d", len(responses))
	for server, count := range responses {
		t.Logf("%s handled %d requests", server, count)
	}
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration benchmark is not enabled")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/data", baseAddress))
			if err != nil {
				b.Error(err)
				return
			}

			if header := resp.Header.Get("lb-from"); header == "" {
				b.Errorf("expected lb-from header, got empty")
			}
		}
	})
}
