package amqlib

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"strconv"
	"sync"

	"time"
)

// Publisher используется для публикации новых сообщений через сервер RabbitMQ.
type Publisher struct {
	Exchange string         // Название точки обмена
	Type     Algoritm       // Тип подписки
	TTL      int            // Время жизни сообщения
	SendTime time.Duration  // Время отводимое на публикацию сообщения
	msgs     chan Published // Канал для отправки сообщений
	eopt     ExchangeOptions
}

var _ Membered = (*Publisher)(nil)

// NewPublisher возвращает новый инициализированный Publisher для публикации сообщений через сервер RabbitMQ.
func NewPublisher(al Algoritm, exchange string, opt *ExchangeOptions, ttl int, sendTime time.Duration, bufSize uint32) *Publisher {
	p := &Publisher{
		Exchange: exchange,
		Type:     al,
		TTL:      ttl,
		SendTime: sendTime,
		msgs:     make(chan Published, bufSize),
	}
	if opt == nil {
		opt = &ExchangeOptions{
			Durable:     true,
			Autodeleted: false,
			Internal:    false,
			Nowait:      false,
			Arguments:   nil,
		}
	}
	p.eopt = *opt
	return p
}

// ErrPushFull возвращается в случае переполнения канала для отсылки сообщений.
var ErrPushFull = errors.New("publication channel is full")

// Push помещает сообщение в канал для отправки.
// Если очередь переполнена и сообщения не отправляются, то возвращается ошибка ErrPushFull.
// Время жизни устанавливается общее
func (p *Publisher) Push(m Published) error {
	// проверяем, что канал не переполнен
	if c := cap(p.msgs); c > 1 && len(p.msgs) == c {
		return ErrPushFull
	}
	/*var (
		exp = strconv.Itoa(p.TTL)
	)
	m.Expiration = exp*/
	// m.Expiration = "10000"
	p.msgs <- m // помещаем сообщение в очередь на отправку

	return nil
}

// Work запускает сервис публикации поверх канала подключения к RabbitMQ.
func (p *Publisher) Work(ctx context.Context, wg *sync.WaitGroup, ch *amqp091.Channel) error {
	// копируем описание элементов Publisher
	var (
		exchange = p.Exchange       // Название точки обмена
		exchType = p.Type.getType() // Тип точки обмена

	)

	// инициализируем описание и проверяем параметры точки обмена
	err := ch.ExchangeDeclare(
		exchange,           // name
		exchType,           // type
		p.eopt.Durable,     // durable
		p.eopt.Autodeleted, // auto-deleted
		p.eopt.Internal,    // internal
		p.eopt.Nowait,      // no-wait
		p.eopt.Arguments,   // arguments
	)
	if err != nil {
		return fmt.Errorf("exchange declare error: %w", err)
	}
	wg.Done()
	for {
		select {
		case <-ctx.Done(): // плановое завершение

			return nil

		case err := <-ch.NotifyClose(make(chan *amqp091.Error)):

			return fmt.Errorf("channel error: %w", err)

		case msgp := <-p.msgs: // сообщение для отправки

			msg, key := msgp.Message(), msgp.Key()
			msg.Expiration = strconv.Itoa(p.TTL)
			ctxm, cancel := context.WithTimeout(ctx, p.SendTime)
			err := ch.PublishWithContext(ctxm, exchange, key, false, false, msg)
			cancel()

			if err != nil {
				return fmt.Errorf("publication error: %w", err)
			}
		}
	}
}
