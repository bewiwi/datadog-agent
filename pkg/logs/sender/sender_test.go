// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package sender

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/datadog-agent/pkg/logs/config"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/logs/sender/mock"
)

func newMessage(content []byte, source *config.LogSource, status string) message.Message {
	origin := message.NewOrigin(source)
	msg := message.New(content, origin, status)
	return msg
}

func TestSender(t *testing.T) {
	l := mock.NewMockLogsIntake(t)
	defer l.Close()

	source := config.NewLogSource("", &config.LogsConfig{})

	input := make(chan message.Message, 1)
	output := make(chan message.Message, 1)
	delimiter := NewDelimiter(false)

	connManager := newConnectionManagerForAddr(l.Addr())
	connManager.Start()

	sender := New(input, output, connManager, delimiter)
	sender.Start()

	expectedMessage := newMessage([]byte("fake line"), source, "")

	// Write to the output should relay the message to the output (after sending it on the wire)
	input <- expectedMessage
	message, ok := <-output

	assert.True(t, ok)
	assert.Equal(t, message, expectedMessage)

	sender.Stop()
	connManager.Stop()
}
