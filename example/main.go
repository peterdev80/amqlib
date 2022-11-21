package main

import (
	"context"
	"fmt"
	"github.com/peterdev80/amqlib"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"
)

// StringMsg принимает строку и преобразует в message
type StringMsg struct {
	ttl int
}

// NewStringMsg создает текстовое сообщение с ttl  отличным от 0
func NewStringMsg(ttl int) *StringMsg {
	return &StringMsg{ttl: ttl}
}

func (str *StringMsg) Message(t interface{}) amqp091.Publishing {
	ret := amqp091.Publishing{
		ContentType: "text",
		Timestamp:   time.Now(),
		Type:        "StringMsg",
		Body:        []byte(t.(string)),
	}
	if str.ttl != 0 {
		ret.Expiration = strconv.Itoa(str.ttl)
	}
	return ret
}

var _ amqlib.Published = (*StringMsg)(nil)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	conect := amqlib.NewConnect(5*time.Second, 5, amqlib.URI("guest", "guest", "localhost", "5672"))

	consumer := amqlib.NewConsumer(amqlib.PubSub, "mn1", nil, nil, func(msg amqp091.Delivery) error {
		fmt.Println(string(msg.Body))
		return nil
	})

	publisher := amqlib.NewPublisher(amqlib.PubSub, "mn1", 300, 20, 1)

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := publisher.Push(NewStringMsg(3000).Message(fmt.Sprint("hello", time.Now())))
				if err != nil {
					panic(err)
				}

			}
		}
	}()

	err := conect.OnTask(ctx, consumer, publisher)

	if err != nil {
		log.Fatal(err)
	}

}
