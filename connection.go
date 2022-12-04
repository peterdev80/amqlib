package amqlib

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"sync"

	"golang.org/x/sync/errgroup"
	"strconv"
	"time"
)

// Addr предназначен для получения строки адреса подключения к rabbit
type Addr string

// URI является вспомогательной функцией, возвращающей строку с описанием схемы для подключения к серверу RabbitMQ.
func URI(user, password, host, port string) Addr {
	p, _ := strconv.Atoi(port) // конвертируем строку с адресом порта в число

	// формируем URI описание подключения к серверу RabbitMQ
	uri := amqp091.URI{
		Scheme:   "amqp",
		Host:     host,
		Port:     p,
		Username: user,
		Password: password,
		Vhost:    "/",
	}

	return Addr(uri.String()) // оборачиваем и возвращаем строку как адрес подключения
}

// Connect подключение к rabbitMQ
type Connect struct {
	reconnectTime time.Duration // Задержка перед повторным соединением
	maxIteration  int           // Максимальное количество попыток
	addr          Addr          // Строка подключения
}

// NewConnect новое подключение к сервису rabbitMQ
// reconnectTime -задержка перед повторным соединением
// maxIteration - максимальное количество попыток соединения
// addr - строка подключения
func NewConnect(reconnectTime time.Duration, maxIteration int, addr Addr) *Connect {
	return &Connect{reconnectTime: reconnectTime, maxIteration: maxIteration, addr: addr}
}

//	Подключение к rabbit
//
// В случае ошибки подключения попытка повторяется несколько раз (maxIteration) с небольшой задержкой (ReconnectTime).
// возвращаем само подключение, или ошибку
func (a Addr) connect(maxIteration int, reconnectTime time.Duration) (*amqp091.Connection, error) {
	var (
		dsn  = string(a)         // Адрес для подключения
		conn *amqp091.Connection // Соединение с сервером
		err  error               // ошибка
	)

	// у нас несколько попыток подключения
	for i := 0; i < maxIteration; i++ {
		conn, err = amqp091.Dial(dsn) // подключаемся к серверу
		if err == nil {
			return conn, nil // в случае успешного подключения сразу возвращаем его
		}

		time.Sleep(reconnectTime) // задержка перед повтором попытки соединения
	}

	// все попытки подключения исчерпаны
	return nil, fmt.Errorf("rabbit connection error: %w", err)

}

// OnTask запуск участников обмена
func (c *Connect) OnTask(ctx context.Context, onstart *sync.WaitGroup, members ...Membered) error {
	var (
		step = 0
		addr = c.addr
		mi   = c.maxIteration
		rt   = c.reconnectTime
	)

	for {

		conn, err := addr.connect(mi, rt)
		if err != nil {
			return fmt.Errorf("connection error: %w", err)
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			select {
			case err = <-conn.NotifyClose(make(chan *amqp091.Error)):
				return fmt.Errorf("connection close: %w", err)

			case <-ctx.Done(): // плановое завершение
				return nil
			}
		})

		for _, member := range members {
			// для каждого сервиса создаём отдельный канал
			ch, err := conn.Channel()
			if err != nil {
				conn.Close()
				// не удалось создать канал на соединение, приложение работать не может
				return fmt.Errorf("channel creation error: %w", err)
			}

			service := member // копируем в текущий стек
			onstart.Add(1)
			// запускаем обработку участника в отдельном потоке
			g.Go(func() error {
				defer ch.Close()

				err := service.Work(gCtx, onstart, ch) // запускаем обработчик сервиса на заданном канале
				if err != nil {
					return fmt.Errorf("work member error: %w", err)
				}
				return nil
			})

		}

		onstart.Done()
		err = g.Wait()

		conn.Close() // закрываем по окончанию

		if gCtx.Err() == nil || errors.Is(gCtx.Err(), context.Canceled) {
			return nil // плановое завершение если завершение по контексту
		}
		if step > c.maxIteration {
			return err
		}
		step++
	}
}
