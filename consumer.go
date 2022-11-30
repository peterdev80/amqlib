package amqlib

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"sync"
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
	Durable     bool
	Autodeleted bool
	Internal    bool
	Nowait      bool
	Arguments   amqp091.Table
}

type ConsumeOptions struct {
	Autoack   bool
	Exclusive bool
	Nolocal   bool
	Nowait    bool
	Arguments amqp091.Table
}

type options struct {
	eopt   ExchangeOptions // Опции для exchange, если nil то используются по умолчанию
	copt   ConsumeOptions  // Опции для consume
	topics []string        // Список поддерживаемых тем
}
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}
func WithExchange(eopt ExchangeOptions) Option {
	return optionFunc(func(o *options) {
		o.eopt = eopt
	})
}
func WithConsume(copt ConsumeOptions) Option {
	return optionFunc(func(o *options) {
		o.copt = copt
	})
}
func WithTopics(topics ...string) Option {
	return optionFunc(func(o *options) {
		o.topics = topics
	})
}

// ConsumerHandler функция для обработки входящего сообщения из rabbitMQ
type ConsumerHandler = func(msg amqp091.Delivery) error

// Consumer описывает обработчик входящих сообщений.
type Consumer struct {
	al   Algoritm // тип exchange
	Name string   // Имя используемое для точки обмена (exchange or queue)
	// eopt *ExchangeOptions // Опции для exchange, если nil то используются по умолчанию
	// copt *ConsumeOptions  // Опции для consume

	handler ConsumerHandler // Обработчик сообщений
	// topics  map[string]struct{} // Список поддерживаемых тем

	Options options
}

// NewConsumer создаем нового слушателя, передав ему
// al - алгоритм распределения
// name - наименования exchange
// handler - функция, которая производит действие с сообщением очереди, функции запускаются в одном потоке,
// для достижения асинхронности использовать отдельную горутину внутри handler
// topics
func NewConsumer(al Algoritm, name string, handler ConsumerHandler, opts ...Option) *Consumer {
	c := &Consumer{al: al, Name: name, handler: handler}

	c.Options = options{
		eopt: ExchangeOptions{
			Durable:     true,
			Autodeleted: false,
			Internal:    false,
			Nowait:      false,
			Arguments:   nil,
		},
		copt: ConsumeOptions{
			Autoack:   true,
			Exclusive: false,
			Nolocal:   false,
			Nowait:    false,
			Arguments: nil,
		},
		topics: nil,
	}
	for _, o := range opts {
		o.apply(&c.Options)
	}

	return c
}

var _ Membered = (*Consumer)(nil)

// initExchange инициализация exchange
func (c *Consumer) initExchange(ch *amqp091.Channel) (string, error) {
	var (
		queName = ""
	)

	if c.al != Queue {

		err := ch.ExchangeDeclare(
			c.Name,                     // name
			c.al.getType(),             // type
			c.Options.eopt.Durable,     // durable
			c.Options.eopt.Autodeleted, // auto-deleted
			c.Options.eopt.Internal,    // internal
			c.Options.eopt.Nowait,      // no-wait
			c.Options.eopt.Arguments,   // arguments
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

func (c *Consumer) Work(ctx context.Context, wg *sync.WaitGroup, channel *amqp091.Channel) error {

	queName, err := c.initExchange(channel)
	if err != nil {
		return err
	}

	switch c.al {
	case Topics, Routing:
		for _, topic := range c.Options.topics {
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
	case PubSub:
		err := channel.QueueBind(
			queName, // queue name
			"",      // routing key
			c.Name,  // exchange
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("queue %q  bind error: %w", queName, err)
		}

	}

	msgs, err := channel.Consume(
		queName,                  // queue
		"",                       // consumer
		c.Options.copt.Autoack,   // auto ack
		c.Options.copt.Exclusive, // exclusive
		c.Options.copt.Nolocal,   // no local
		c.Options.copt.Nowait,    // no wait
		c.Options.copt.Arguments, // args
	)
	if err != nil {
		return fmt.Errorf("consume initialization error: %w", err)
	}

	wg.Done()
	for {
		select {
		case <-ctx.Done(): // плановое завершение

			return nil

		case err := <-channel.NotifyClose(make(chan *amqp091.Error)):

			return fmt.Errorf(" work channel error: %w", err)

		case m := <-msgs: // входящие сообщения

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
