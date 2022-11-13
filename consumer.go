package amqlib

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
)

type Algoritm int

const (
	Queue Algoritm = iota
	PubSub
	Routing
	Topics
)

// getType преобразовать в string rabbit
func (a Algoritm) getType() string {
	switch a {
	case PubSub:
		return "fanout"
	case Routing:
		return "direct"
	case Topics:
		return "topic"
	}
	return ""
}

// ExchangeOptions опции которые необходимо установить exchange
type ExchangeOptions struct {
	durable     bool
	autodeleted bool
	internal    bool
	nowait      bool
	arguments   amqp091.Table
}

type ConsumeOptions struct {
	autoack   bool
	exclusive bool
	nolocal   bool
	nowait    bool
	arguments amqp091.Table
}

func NewConsumeOptions(autoack bool, exclusive bool, nolocal bool, nowait bool, arguments amqp091.Table) *ConsumeOptions {
	return &ConsumeOptions{autoack: autoack, exclusive: exclusive, nolocal: nolocal, nowait: nowait, arguments: arguments}
}

func NewExchangeOptions(durable bool, autodeleted bool, internal bool, nowait bool, arguments amqp091.Table) *ExchangeOptions {
	return &ExchangeOptions{durable: durable, autodeleted: autodeleted, internal: internal, nowait: nowait, arguments: arguments}
}

type ConsumerHandler = func(msg amqp091.Delivery) error

// Consumer описывает обработчик входящих сообщений.
type Consumer struct {
	al   Algoritm
	Name string           // Имя используемое для точки обмена (exchange or queue)
	eopt *ExchangeOptions // Опции для exchange, если nil то используются по умолчанию
	copt *ConsumeOptions  // Опции для consume

	handler ConsumerHandler     // Обработчик сообщений
	topics  map[string]struct{} // Список поддерживаемых тем
	// connected bool // Флаг активного соединения

	// topicAddCh chan string // Канал для подписки на новые темы
	// topicDelCh chan string // Канал для отписки
}

var _ Membered = (*Consumer)(nil)

// initExchange инициализация exchange
func (c *Consumer) initExchange(ch *amqp091.Channel) (string, error) {
	var (
		queName = ""
	)

	if c.al != Queue {

		if c.eopt == nil {
			c.eopt = NewExchangeOptions(true, false, false, false, nil)
		}

		err := ch.ExchangeDeclare(
			c.Name,             // name
			c.al.getType(),     // type
			c.eopt.durable,     // durable
			c.eopt.autodeleted, // auto-deleted
			c.eopt.internal,    // internal
			c.eopt.nowait,      // no-wait
			c.eopt.arguments,   // arguments
		)
		if err != nil {
			return "", fmt.Errorf("exchange declare error: %w", err)
		}
	} else {
		queName = c.Name
	}

	q, err := ch.QueueDeclare(
		queName, // name
		false,   // durable
		false,   // delete when unused
		true,    // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return "", fmt.Errorf("queue declare error: %w", err)
	}
	return q.Name, nil
}

func (c *Consumer) Work(ctx context.Context, channel *amqp091.Channel) error {
	queName, err := c.initExchange(channel)
	if err != nil {
		return err
	}
	if c.copt == nil {
		c.copt = NewConsumeOptions(true, false, false, false, nil)
	}
	msgs, err := channel.Consume(
		queName,          // queue
		"",               // consumer
		c.copt.autoack,   // auto ack
		c.copt.exclusive, // exclusive
		c.copt.nolocal,   // no local
		c.copt.nowait,    // no wait
		c.copt.arguments, // args
	)
	if err != nil {
		return fmt.Errorf("consume initialization error: %w", err)
	}

	if c.al == Topics {
		for topic := range c.topics {
			err := channel.QueueBind(
				queName, // queue name
				topic,   // routing key
				c.Name,  // exchange
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("queue %q topic %q bind error: %w", queName, topic, err)
			}
		}
	}

	for {
		select {
		case <-ctx.Done(): // плановое завершение

			return nil

		case err := <-channel.NotifyClose(make(chan *amqp091.Error)):

			return fmt.Errorf(" work channel error: %w", err)

		case m, ok := <-msgs: // входящие сообщения
			if !ok {
				return nil // канал с сообщениями закрыт
			}

			// вызываем обработчик события
			if err := c.handler(m); err != nil {

				return fmt.Errorf("work message handle error: %w", err)
			}

			/*case topic := <-c.topicAddCh:
				err := channel.QueueBind(
					queName, // queue name
					topic,   // routing key
					c.Name,  // exchange
					false,
					nil)
				if err != nil {
					return fmt.Errorf("queue %q topic %q bind error: %w", queName, topic, err)
				}

			case topic := <-c.topicDelCh:
				err := channel.QueueUnbind(
					queName, // queue name
					topic,   // routing key
					c.Name,  // exchange
					nil)
				if err != nil {
					return fmt.Errorf("queue %q topic %q unbind error: %w", queName, topic, err)
				}*/
		}
	}

}
