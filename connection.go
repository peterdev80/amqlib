package amqlib

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"strconv"
	"time"
)

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

func NewConnect(reconnectTime time.Duration, maxIteration int, addr Addr) *Connect {
	return &Connect{reconnectTime: reconnectTime, maxIteration: maxIteration, addr: addr}
}

//  Подключение к rabbit
// В случае ошибки подключения попытка повторяется несколько раз (MaxIteration) с небольшой задержкой (ReconnectTime).
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

		log.Debug().
			Err(err).
			Int("iteration", i+1).
			Dur("delay", reconnectTime).
			Msg("connection iteration")

		time.Sleep(reconnectTime) // задержка перед повтором попытки соединения
	}

	// все попытки подключения исчерпаны
	return nil, fmt.Errorf("rabbit connection error: %w", err)

}

// OnTask запуск участников обмена
func (c *Connect) OnTask(ctx context.Context, members ...Membered) error {
	step := 0
	addr := c.addr
	mi := c.maxIteration
	rt := c.reconnectTime
	for {
		log.Info().Int("step", step).Msg("run OnTask")
		conn, err := addr.connect(mi, rt)
		if err != nil {
			return fmt.Errorf("connection error: %w", err)
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			select {
			case err = <-conn.NotifyClose(make(chan *amqp091.Error)):
				return fmt.Errorf("connection close: %w", err)

			case <-gCtx.Done(): // плановое завершение
				err := gCtx.Err()
				return fmt.Errorf("connection done: %w", err)
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
			// запускаем обработку участника в отдельном потоке
			g.Go(func() error {
				defer ch.Close()
				err := service.Work(gCtx, ch) // запускаем обработчик сервиса на заданном канале
				if err != nil {
					return fmt.Errorf("member error: %w", err)
				}
				return nil
			})

		}

		err = g.Wait()

		conn.Close() // закрываем по окончанию

		if gCtx.Err() == nil {
			return nil // плановое завершение если завершение по контексту
		}

	}
}
