package amqlib_test

import (
	"context"
	"fmt"
	"github.com/peterdev80/amqlib"
	"github.com/rabbitmq/amqp091-go"
	"os"
	"os/signal"
	"sync"
	"time"
)

// StringMsg принимает строку и преобразует в message
type StringMsg struct {
	data  string
	topic string
}

// NewStringMsg создает текстовое сообщение с
func NewStringMsg(data, topic string) *StringMsg {
	return &StringMsg{data: data, topic: topic}
}
func (str *StringMsg) Key() string {
	return str.topic
}
func (str *StringMsg) Message() amqp091.Publishing {
	ret := amqp091.Publishing{
		ContentType: "text",
		Timestamp:   time.Now(),
		Type:        "StringMsg",
		Body:        []byte(str.data),
	}

	return ret
}
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	conect := amqlib.NewConnect(5*time.Second, 5, amqlib.URI("guest", "guest", "localhost", "5672"))
	wg := &sync.WaitGroup{}
	wg.Add(5)
	consumer := amqlib.NewConsumer(amqlib.PubSub, "mn1", func(msg amqp091.Delivery) error {

		fmt.Println("msg->", string(msg.Body))
		wg.Done()
		return nil
	})

	publisher := amqlib.NewPublisher(amqlib.PubSub, "mn1", nil, 300, 20, 1)
	onstart := &sync.WaitGroup{}
	onstart.Add(1)
	go func() {
		err := conect.OnTask(ctx, onstart, consumer, publisher)

		if err != nil {
			panic(err)
		}
	}()

	onstart.Wait()
	for i := 0; i < 5; i++ {
		err := publisher.Push(NewStringMsg(fmt.Sprint("hello fanout ", i), ""))
		if err != nil {
			panic(err)
		}
	}
	wg.Wait()

	// Output:
	// msg-> hello fanout 0
	// msg-> hello fanout 1
	// msg-> hello fanout 2
	// msg-> hello fanout 3
	// msg-> hello fanout 4

}
