// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package sender

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/net/proxy"

	"github.com/DataDog/datadog-agent/pkg/util/log"

	"github.com/DataDog/datadog-agent/pkg/logs/config"
)

const (
	backoffUnit       = 2 * time.Second
	backoffMax        = 30 * time.Second
	connectionTimeout = 20 * time.Second
)

// A ConnectionManager manages connections
type ConnectionManager struct {
	serverConfig *config.ServerConfig
	proxyAddress string
	mutex        sync.Mutex
	firstConn    sync.Once
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewConnectionManager returns an initialized ConnectionManager
func NewConnectionManager(serverConfig *config.ServerConfig, proxyAddress string) *ConnectionManager {
	return &ConnectionManager{
		serverConfig: serverConfig,
		proxyAddress: proxyAddress,
	}
}

// Start starts the connection manager.
func (cm *ConnectionManager) Start() {
	cm.ctx, cm.cancel = context.WithCancel(context.Background())
}

// Stop stops the connection manager.
func (cm *ConnectionManager) Stop() {
	// This will make sure we don't attempt new connections.
	cm.cancel()
}

// Context return the current context.
func (cm *ConnectionManager) Context() context.Context {
	return cm.ctx
}

// NewConnection returns an initialized connection to the intake.
// It blocks until a connection is available
func (cm *ConnectionManager) NewConnection(ctx context.Context) (net.Conn, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.firstConn.Do(func() {
		if cm.proxyAddress != "" {
			log.Infof("Connecting to the backend: %v, via socks5: %v, with SSL: %v", cm.serverConfig.Address(), cm.proxyAddress, cm.serverConfig.UseSSL)
		} else {
			log.Infof("Connecting to the backend: %v, with SSL: %v", cm.serverConfig.Address(), cm.serverConfig.UseSSL)
		}
	})

	var retries int
	for {
		if retries > 0 {
			log.Debugf("Connect attempt #%d", retries)
			cm.backoff(ctx, retries)
		}
		retries++

		// Check if we should continue.
		select {
		// This is the normal shutdown path when the caller is stopped.
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Continue.
		}

		var conn net.Conn
		var err error

		if cm.proxyAddress != "" {
			var dialer proxy.Dialer
			dialer, err = proxy.SOCKS5("tcp", cm.proxyAddress, nil, proxy.Direct)
			if err != nil {
				log.Warn(err)
				continue
			}
			// TODO: handle timeouts with ctx.
			conn, err = dialer.Dial("tcp", cm.serverConfig.Address())
		} else {
			var dialer net.Dialer
			dctx, cancel := context.WithTimeout(ctx, connectionTimeout)
			defer cancel()
			conn, err = dialer.DialContext(dctx, "tcp", cm.serverConfig.Address())
		}
		if err != nil {
			log.Warn(err)
			continue
		}
		log.Debug("Connected")

		if cm.serverConfig.UseSSL {
			sslConn := tls.Client(conn, &tls.Config{
				ServerName: cm.serverConfig.Name,
			})
			// TODO: Handle timeouts with ctx.
			err = sslConn.Handshake()
			if err != nil {
				log.Warn(err)
				continue
			}
			log.Debug("SSL handshake successful")
			conn = sslConn
		}

		// Add a watchdog for the connection.
		go cm.handleServerClose(conn)
		return conn, nil
	}
}

// CloseConnection closes a connection on the client side
func (cm *ConnectionManager) CloseConnection(conn net.Conn) {
	conn.Close()
	log.Info("Connection closed")
}

// handleServerClose lets the connection manager detect when a connection
// has been closed by the server, and closes it for the client.
// This is not strictly necessary but a good safeguard against callers
// that might not handle errors properly.
func (cm *ConnectionManager) handleServerClose(conn net.Conn) {
	for {
		buff := make([]byte, 1)
		_, err := conn.Read(buff)
		if err == io.EOF {
			cm.CloseConnection(conn)
			return
		} else if err != nil {
			log.Warn(err)
			return
		}
	}
}

// backoff lets the connection manager sleep a bit
func (cm *ConnectionManager) backoff(ctx context.Context, retries int) {
	backoffDuration := backoffUnit * time.Duration(retries)
	if backoffDuration > backoffMax {
		backoffDuration = backoffMax
	}

	ctx, cancel := context.WithTimeout(ctx, backoffDuration)
	defer cancel()
	<-ctx.Done()
}
