package amqlib

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"sync"
)

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

	ops options
}

// NewConsumer создаем нового слушателя, передав ему
// al - алгоритм распределения
// name - наименования exchange
// handler - функция, которая производит действие с сообщением очереди, функции запускаются в одном потоке,
// для достижения асинхронности использовать отдельную горутину внутри handler
// topics
func NewConsumer(al Algoritm, name string, handler ConsumerHandler, opts ...Option) *Consumer {
	c := &Consumer{al: al, Name: name, handler: handler}

	c.ops = options{
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
		qopt: QueueOptions{
			Durable:     false,
			Autodeleted: false,
			Exclusive:   true,
			Nowait:      false,
			Arguments:   nil,
		},
		topics: nil,
		qos:    nil,
	}
	for _, o := range opts {
		o.apply(&c.ops)
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
			c.Name,                 // name
			c.al.getType(),         // type
			c.ops.eopt.Durable,     // durable
			c.ops.eopt.Autodeleted, // auto-deleted
			c.ops.eopt.Internal,    // internal
			c.ops.eopt.Nowait,      // no-wait
			c.ops.eopt.Arguments,   // arguments
		)
		if err != nil {
			return "", fmt.Errorf("exchange declare error: %w", err)
		}
	} else {
		queName = c.Name
	}

	q, err := ch.QueueDeclare(
		queName,                // name
		c.ops.qopt.Durable,     // durable
		c.ops.qopt.Autodeleted, // delete when unused
		c.ops.qopt.Exclusive,   // exclusive
		c.ops.qopt.Nowait,      // no-wait
		c.ops.qopt.Arguments,   // arguments
	)
	if err != nil {
		return "", fmt.Errorf("queue declare error: %w", err)
	}

	if c.ops.qos != nil {
		err = ch.Qos(
			c.ops.qos.PCount, // prefetch count
			c.ops.qos.PSize,  // prefetch size
			c.ops.qos.Global, // global
		)
		if err != nil {
			return "", fmt.Errorf("queue declare qos error: %w", err)
		}
	}

	return q.Name, nil
}

// Work реализация интерфейса Membered, работа по чтению сообщений
func (c *Consumer) Work(ctx context.Context, wg *sync.WaitGroup, channel *amqp091.Channel) error {

	queName, err := c.initExchange(channel)
	if err != nil {
		return err
	}

	switch c.al {
	case Topics, Routing:
		for _, topic := range c.ops.topics {
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
		queName,              // queue
		"",                   // consumer
		c.ops.copt.Autoack,   // auto ack
		c.ops.copt.Exclusive, // exclusive
		c.ops.copt.Nolocal,   // no local
		c.ops.copt.Nowait,    // no wait
		c.ops.copt.Arguments, // args
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
			if !c.ops.copt.Autoack {
				if err = m.Ack(false); err != nil {
					return fmt.Errorf("work message ask handle error: %w", err)
				}
			}

		}
	}

}
