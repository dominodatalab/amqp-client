package amqpclient

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	logger  = logr.Discard()
	amqpURL = "amqp://guest:guest@127.0.0.1:5672"
)

func TestSmoke(t *testing.T) {
	client, err := NewSimpleClient(logger, amqpURL)
	require.NoError(t, err)

	err = client.Publish(context.Background(), SimpleMessage{
		AppID:        "client-test",
		ExchangeName: "non-default",
		QueueName:    "steve-o",
		ContentType:  "application/json",
		Body:         []byte(`{"success":"ok"}`),
	})
	assert.NoError(t, err)
}

func TestNewSimpleClient(t *testing.T) {
	t.Run("bad_addr", func(t *testing.T) {
		addrs := []string{
			"steve",
			"amqp://steve",
			"amqp://steve:o@127.0.0.1:5672",
		}
		for _, addr := range addrs {
			_, err := NewSimpleClient(logger, addr)
			assert.Error(t, err)
		}
	})
}

func TestSimpleClientPublish(t *testing.T) {
}

func TestSimpleClientClose(t *testing.T) {
}
