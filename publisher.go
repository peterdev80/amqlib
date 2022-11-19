package amqlib

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/amqp091-go"

	"github.com/rs/zerolog/log"
	"strconv"
	"time"
)

// Publisher используется для публикации новых сообщений через сервер RabbitMQ.
type Publisher struct {
	Exchange string                  // Название точки обмена
	Type     Algoritm                // Тип подписки
	TTL      int                     // Время жизни сообщения
	SendTime time.Duration           // Время отводимое на публикацию сообщения
	msgs     chan amqp091.Publishing // Канал для отправки сообщений

}

var _ Membered = (*Publisher)(nil)

// NewPublisher возвращает новый инициализированный Publisher для публикации сообщений через сервер RabbitMQ.
func NewPublisher(al Algoritm, exchange string, ttl int, sendTime time.Duration, bufSize uint32) *Publisher {
	return &Publisher{
		Exchange: exchange,
		Type:     al,
		TTL:      ttl,
		SendTime: sendTime,
		msgs:     make(chan amqp091.Publishing, bufSize),
	}
}

// ErrPushFull возвращается в случае переполнения канала для отсылки сообщений.
var ErrPushFull = errors.New("publication channel is full")

// Push помещает сообщение в канал для отправки.
// Если очередь переполнена и сообщения не отправляются, то возвращается ошибка ErrPushFull.
// Время жизни устанавливается общее
func (p *Publisher) Push(m amqp091.Publishing) error {
	// проверяем, что канал не переполнен
	if c := cap(p.msgs); c > 0 && len(p.msgs) == c {
		return ErrPushFull
	}
	var (
		exp = strconv.Itoa(p.TTL)
	)
	m.Expiration = exp
	m.Expiration = "10000"
	p.msgs <- m // помещаем сообщение в очередь на отправку

	return nil
}

// Work запускает сервис публикации поверх канала подключения к RabbitMQ.
func (p *Publisher) Work(ctx context.Context, ch *amqp091.Channel) error {
	// копируем описание элементов Publisher
	var (
		exchange = p.Exchange       // Название точки обмена
		exchType = p.Type.getType() // Тип точки обмена

	)

	// инициализируем описание и проверяем параметры точки обмена
	err := ch.ExchangeDeclare(
		exchange, // name
		exchType, // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return fmt.Errorf("exchange declare error: %w", err)
	}

	for {
		select {
		case <-ctx.Done(): // плановое завершение
			log.Debug().Msg("publisher context stop")

			return nil

		case err := <-ch.NotifyClose(make(chan *amqp091.Error)):
			log.Debug().
				Err(err).
				Msg("publisher notify close")

			return fmt.Errorf("channel error: %w", err)

		case msg := <-p.msgs: // сообщение для отправки

			ctxm, cancel := context.WithTimeout(ctx, p.SendTime)
			var key string
			if v, ok := msg.Headers["key"]; ok {
				key = v.(string)
			}

			err := ch.PublishWithContext(ctxm, exchange, key, false, false, msg)
			cancel()

			log.Debug().
				Err(err).
				Msg("publisher send message")

			if err != nil {
				return fmt.Errorf("publication error: %w", err)
			}
		}
	}
}
