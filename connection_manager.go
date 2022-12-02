package amqpclient

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/go-logr/logr"
	"github.com/rabbitmq/amqp091-go"
)

const (
	dialTimeout   = 10 * time.Second
	retryDelay    = 1 * time.Second
	retryMaxDelay = 30 * time.Second
)

var (
	ErrNotReady              = errors.New("connection/channel not ready for use")
	ErrAlreadyClosed         = errors.New("manager already closed: not connected to server")
	ErrServerAckTimeout      = errors.New("waiting for server message ack timed out")
	ErrNoMessageConfirmation = errors.New("server did not confirm message receipt")
)

type ConnectionManager struct {
	logger          logr.Logger
	channel         *amqp091.Channel
	connection      *amqp091.Connection
	notifyConnClose chan *amqp091.Error
	notifyChanClose chan *amqp091.Error
	notifyPublish   chan amqp091.Confirmation
	done            chan bool
	isReady         bool
	redactedAddr    string
}

func NewConnectionManager(log logr.Logger, addr string) (*ConnectionManager, error) {
	// sanity check ensures connection params are valid before starting connect/open loops
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	_ = conn.Close()
	u, _ := url.Parse(addr)

	client := &ConnectionManager{
		logger:       log.WithName("connection-manager"),
		redactedAddr: u.Redacted(),
		done:         make(chan bool),
	}
	go client.handleReconnect(addr)

	return client, nil
}

func (m *ConnectionManager) Channel() (*amqp091.Channel, error) {
	select {
	case <-m.done:
		return nil, ErrAlreadyClosed
	default:
	}

	if m.channel == nil || m.channel.IsClosed() || !m.isReady {
		return nil, ErrNotReady
	}
	return m.channel, nil
}

func (m *ConnectionManager) WithChannel(fn func(ch *amqp091.Channel) error) error {
	var ch *amqp091.Channel

	err := retry.Do(
		func() error {
			var e error
			ch, e = m.Channel()

			return e
		},
		retry.RetryIf(func(e error) bool {
			return e != ErrAlreadyClosed
		}),
		retry.Attempts(3),
		retry.LastErrorOnly(true),
	)

	if err != nil {
		return err
	}

	return fn(ch)
}

func (m *ConnectionManager) WithChannelAndConfirmation(fn func(ch *amqp091.Channel) error) error {
	return m.WithChannel(func(ch *amqp091.Channel) error {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("cannot put channel into confirmation mode: %w", err)
		}

		if err := fn(ch); err != nil {
			return err
		}

		select {
		case <-time.After(serverAckTimeout):
			return ErrServerAckTimeout
		case confirm := <-m.notifyPublish:
			if confirm.Ack {
				m.logger.Info("Publish confirmed", "deliveryTag", confirm.DeliveryTag)
				return nil
			}

			return ErrNoMessageConfirmation
		}
	})
}

func (m *ConnectionManager) Close() error {
	select {
	case <-m.done:
		return nil
	default:
	}

	m.logger.Info("Shutting down")
	close(m.done)

	if err := m.channel.Close(); err != nil {
		return err
	}
	if err := m.connection.Close(); err != nil {
		return err
	}

	m.isReady = false
	m.logger.Info("Shutdown complete")

	return nil
}

func (m *ConnectionManager) handleReconnect(addr string) {
	for {
		m.isReady = false
		m.logger.Info("Attempting to connect to AMQP server", "addr", m.redactedAddr)

		m.retryUntilSuccessful(
			func() error {
				return m.connect(addr)
			},
			func(n uint, err error) {
				m.logger.Error(err, "Failed to connect, retrying", "attempt", n)
			},
		)

		if done := m.handleReopen(); done {
			break
		}
	}
}

func (m *ConnectionManager) handleReopen() bool {
	for {
		m.isReady = false
		m.logger.Info("Attempting to open AMQP channel")

		m.retryUntilSuccessful(
			m.open,
			func(n uint, err error) {
				m.logger.Error(err, "Failed to open channel, retrying", "attempt", n)
			},
		)

		select {
		case <-m.done:
			return true
		case err := <-m.notifyConnClose:
			m.logger.Error(err, "Connection closed, attempting to reconnect")
			return false
		case err := <-m.notifyChanClose:
			m.logger.Error(err, "Channel closed, attempting to reopen")
		}
	}
}

func (m *ConnectionManager) retryUntilSuccessful(retryable retry.RetryableFunc, onRetry retry.OnRetryFunc) {
	_ = retry.Do(
		retryable,
		retry.Attempts(0),
		retry.Delay(retryDelay),
		retry.MaxDelay(retryMaxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(onRetry),
		retry.RetryIf(func(_ error) bool {
			select {
			case <-m.done:
				return false
			default:
				return true
			}
		}),
	)
}

func (m *ConnectionManager) connect(addr string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	m.logger.Info("Connection established")

	m.connection = conn
	m.notifyConnClose = m.connection.NotifyClose(make(chan *amqp091.Error, 1))

	return nil
}

func (m *ConnectionManager) open() error {
	ch, err := m.connection.Channel()
	if err != nil {
		return err
	}
	m.logger.Info("Channel opened")

	m.channel = ch
	m.notifyChanClose = m.channel.NotifyClose(make(chan *amqp091.Error, 1))
	m.notifyPublish = m.channel.NotifyPublish(make(chan amqp091.Confirmation))
	m.isReady = true

	return nil
}

func dial(addr string) (*amqp091.Connection, error) {
	conn, err := amqp091.DialConfig(addr, amqp091.Config{Dial: amqp091.DefaultDial(dialTimeout)})
	if err != nil {
		err = fmt.Errorf("amqp dial failed: %w", err)
	}

	return conn, err
}
