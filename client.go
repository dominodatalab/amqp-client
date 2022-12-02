package amqpclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/rabbitmq/amqp091-go"
)

const (
	publishMandatoryDelivery = true
	publishImmediateDelivery = false

	exchangeType       = "direct"
	exchangeDurable    = true
	exchangeAutoDelete = false
	exchangeInternal   = false
	exchangeNoWait     = false

	queueDurable    = true
	queueAutoDelete = false
	queueExclusive  = false
	queueNoWait     = false

	serverAckTimeout = 10 * time.Second
)

var queueArgs = amqp091.Table{"x-single-active-consumer": true}

type SimpleMessage struct {
	AppID        string
	ExchangeName string
	QueueName    string
	ContentType  string
	Body         []byte
}

type SimpleClient struct {
	Manager *ConnectionManager
	logger  logr.Logger
}

func NewSimpleClient(log logr.Logger, addr string) (*SimpleClient, error) {
	manager, err := NewConnectionManager(log, addr)
	if err != nil {
		return nil, fmt.Errorf("cannot create connection manager: %w", err)
	}

	return &SimpleClient{
		Manager: manager,
		logger:  log.WithName("simple-client"),
	}, nil
}

func (c *SimpleClient) Publish(ctx context.Context, msg SimpleMessage) error {
	if err := c.ensureExchange(msg.ExchangeName); err != nil {
		return err
	}
	if err := c.ensureQueue(msg.ExchangeName, msg.QueueName); err != nil {
		return err
	}

	message := amqp091.Publishing{
		AppId:        msg.AppID,
		Timestamp:    time.Now(),
		DeliveryMode: amqp091.Persistent,
		ContentType:  msg.ContentType,
		Body:         msg.Body,
	}

	c.logger.Info("Sending message to server", "exchange", msg.ExchangeName, "queue", msg.QueueName)
	err := c.Manager.WithChannelAndConfirmation(func(ch *amqp091.Channel) error {
		return ch.PublishWithContext(
			ctx,
			msg.ExchangeName,
			msg.QueueName,
			publishMandatoryDelivery,
			publishImmediateDelivery,
			message,
		)
	})
	if err != nil {
		return fmt.Errorf("message publishing failed: %w", err)
	}

	return nil
}

func (c *SimpleClient) Close() error {
	return c.Manager.Close()
}

func (c *SimpleClient) ensureExchange(exchange string) error {
	if exchange == "" {
		c.logger.V(1).Info("No exchange name provided, using AMQP default")
		return nil
	}

	err := c.Manager.WithChannel(func(ch *amqp091.Channel) error {
		return ch.ExchangeDeclarePassive(
			exchange,
			exchangeType,
			exchangeDurable,
			exchangeAutoDelete,
			exchangeInternal,
			exchangeNoWait,
			nil,
		)
	})
	if isNotFound(err) {
		err = c.Manager.WithChannel(func(ch *amqp091.Channel) error {
			return ch.ExchangeDeclare(
				exchange,
				exchangeType,
				exchangeDurable,
				exchangeAutoDelete,
				exchangeInternal,
				exchangeNoWait,
				nil,
			)
		})
		if err != nil {
			return fmt.Errorf("cannot declare exchange: %w", err)
		}
	}

	return err
}

func (c *SimpleClient) ensureQueue(exchange, queue string) error {
	if queue == "" {
		c.logger.V(1).Info("No queue name provided, skipping creation and binding")
		return nil
	}

	err := c.Manager.WithChannel(func(ch *amqp091.Channel) error {
		_, err := ch.QueueInspect(queue)
		return err
	})
	if isNotFound(err) {
		err = c.Manager.WithChannel(func(ch *amqp091.Channel) error {
			_, e := ch.QueueDeclare(
				queue,
				queueDurable,
				queueAutoDelete,
				queueExclusive,
				queueNoWait,
				queueArgs,
			)
			return e
		})
		if err != nil {
			return fmt.Errorf("cannot declare queue: %w", err)
		}

		if exchange == "" {
			return nil
		}

		err = c.Manager.WithChannel(func(ch *amqp091.Channel) error {
			return ch.QueueBind(queue, queue, exchange, queueNoWait, nil)
		})
		if err != nil {
			return fmt.Errorf("cannot bind queue to exchange: %w", err)
		}
	}

	return err
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}

	var ae *amqp091.Error
	return errors.As(err, &ae) && strings.HasPrefix(ae.Reason, "NOT_FOUND")
}
