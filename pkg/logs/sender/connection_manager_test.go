// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package sender

import (
	"github.com/DataDog/datadog-agent/pkg/logs/sender/mock"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/datadog-agent/pkg/logs/config"
)

func newConnectionManagerForAddr(addr net.Addr) *ConnectionManager {
	host, port := mock.AddrToHostPort(addr)
	return newConnectionManagerForHostPort(host, port)
}

func newConnectionManagerForHostPort(host string, port int) *ConnectionManager {
	serverConfig := config.NewServerConfig(host, port, false)
	return NewConnectionManager(serverConfig, "")
}

func TestNewConnection(t *testing.T) {
	l := mock.NewMockLogsIntake(t)
	defer l.Close()

	connManager := newConnectionManagerForAddr(l.Addr())
	connManager.Start()
	defer connManager.Stop()

	conn, err := connManager.NewConnection(connManager.Context())
	assert.NotNil(t, conn)
	assert.NoError(t, err)
}

func TestNewConnectionReturnsWhenContextCancelled(t *testing.T) {
	connManager := newConnectionManagerForHostPort("foo", 0)
	connManager.Start()

	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		conn, err := connManager.NewConnection(connManager.Context())
		assert.Nil(t, conn)
		assert.Error(t, err)
		wg.Done()
	}()

	// This will cancel the context and should unblock new connection.
	connManager.Stop()

	// Make sure NewConnection really returns.
	wg.Wait()
}
