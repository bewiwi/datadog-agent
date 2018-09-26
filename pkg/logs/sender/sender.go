// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package sender

import (
	"context"
	"github.com/DataDog/datadog-agent/pkg/logs/config"
	"net"
	"time"

	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

// A Sender sends messages from an inputChan to datadog's intake,
// handling connections and retries. Senders are non-reusable once stopped.
type Sender struct {
	inputChan   chan message.Message
	outputChan  chan message.Message
	connManager *ConnectionManager
	delimiter   Delimiter
	done        chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	conn        net.Conn
}

// New returns an initialized Sender
func New(inputChan, outputChan chan message.Message, connManager *ConnectionManager, delimiter Delimiter) *Sender {
	return &Sender{
		inputChan:   inputChan,
		outputChan:  outputChan,
		connManager: connManager,
		delimiter:   delimiter,
		done:        make(chan struct{}),
	}
}

// Start starts the Sender
func (s *Sender) Start() {
	// A sender depends on its connection manager context as a way to allow non-graceful stops.
	s.ctx, s.cancel = context.WithCancel(s.connManager.Context())
	go s.run()
}

// Stop stops the Sender,
// this call blocks until inputChan is flushed
func (s *Sender) Stop() {
	close(s.inputChan)

	timeout := config.LogsAgent.GetDuration("logs_config.stop_grace_period")

	// Either wait for done, for maximum stopTimeout.
	select {
	case <-s.done:
	case <-time.After(timeout):
		// Force cancellation and wait again.
		log.Info("could not flush sender, dropping in-flight messages")
		s.cancel()
		<-s.done
	}

	// Cleanups to make it re-usable safely.
	s.conn = nil
	s.ctx = nil
	s.cancel = nil
}

// run lets the sender wire messages
func (s *Sender) run() {
	defer func() {
		s.done <- struct{}{}
	}()
	for payload := range s.inputChan {
		select {
		case <-s.ctx.Done():
			// If we need to exit we must still read all the input chan to unblock our producers.
			continue
		default:
		}

		s.wireMessage(payload)
	}
}

// wireMessage lets the Sender send a message to datadog's intake
func (s *Sender) wireMessage(payload message.Message) {
	for {
		if s.conn == nil {
			// blocks until a new conn is ready
			s.conn, _ = s.connManager.NewConnection(s.ctx)
		}

		// This happens when the connection attempt has been interrupted (whe stopping).
		if s.conn == nil {
			break
		}

		frame, err := s.delimiter.delimit(payload.Content())
		if err != nil {
			log.Error("can't serialize payload: ", payload, err)
			continue
		}
		_, err = s.conn.Write(frame)
		if err != nil {
			log.Error("can't send payload: ", err)
			s.connManager.CloseConnection(s.conn)
			s.conn = nil
			continue
		}
		// Send it back to the auditor to commit the offset.
		s.outputChan <- payload
		return
	}
}
