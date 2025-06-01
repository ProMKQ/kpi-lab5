package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLeastConnServer_SingleHealthy(t *testing.T) {
	serversPool = []*Server{
		{Address: "server1", ConnCnt: 3, Healthy: false},
		{Address: "server2", ConnCnt: 2, Healthy: true},
		{Address: "server3", ConnCnt: 1, Healthy: false},
	}

	server := getLeastConnServer()
	assert.NotNil(t, server)
	assert.Equal(t, "server2", server.Address)
}

func TestGetLeastConnServer_MultipleHealthy(t *testing.T) {
	serversPool = []*Server{
		{Address: "server1", ConnCnt: 5, Healthy: true},
		{Address: "server2", ConnCnt: 2, Healthy: true},
		{Address: "server3", ConnCnt: 7, Healthy: true},
	}

	server := getLeastConnServer()
	assert.NotNil(t, server)
	assert.Equal(t, "server2", server.Address)
}

func TestGetLeastConnServer_NoHealthy(t *testing.T) {
	serversPool = []*Server{
		{Address: "server1", ConnCnt: 1, Healthy: false},
		{Address: "server2", ConnCnt: 2, Healthy: false},
	}

	server := getLeastConnServer()
	assert.Nil(t, server)
}

func TestGetLeastConnServer_EqualConn(t *testing.T) {
	serversPool = []*Server{
		{Address: "server1", ConnCnt: 2, Healthy: true},
		{Address: "server2", ConnCnt: 2, Healthy: true},
	}

	server := getLeastConnServer()
	assert.NotNil(t, server)
	assert.Contains(t, []string{"server1", "server2"}, server.Address)
}
