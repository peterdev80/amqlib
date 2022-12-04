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

// Example_pub пример работы с pub/sub реализацией rabbit
func Example_pub() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	// создаем новое подключение к rabbit (rabbit предварительно запущен из example)
	conect := amqlib.NewConnect(5*time.Second, 5, amqlib.URI("guest", "guest", "localhost", "5672"))
	// определяем когда надо прервать тест по waitgroup
	wg := &sync.WaitGroup{}
	wg.Add(5)

	// слушатель с инициализированными данными по умолчанию exchange mn1 и handler выводящий данные
	consumer := amqlib.NewConsumer(amqlib.PubSub, "mn1", func(msg amqp091.Delivery) error {

		fmt.Println("msg->", string(msg.Body))
		wg.Done()
		return nil
	})

	// издатель инициализированный на exchange mn1
	publisher := amqlib.NewPublisher(amqlib.PubSub, "mn1", nil, 300, 20, 1)
	onstart := &sync.WaitGroup{}
	onstart.Add(1)
	go func() {
		// Осуществляется связь соединения и элементов.
		// ! Слушателя и издателя рекомендовано применять с разными соединениями !
		err := conect.OnTask(ctx, onstart, consumer, publisher)

		if err != nil {
			panic(err)
		}
	}()

	onstart.Wait()
	for i := 0; i < 5; i++ {
		// пример публикации сообщения
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

// Example_workQueues пример публикации сообщения в очередь и использование опций отличных от дефолтных
func Example_workQueues() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	conect := amqlib.NewConnect(5*time.Second, 5, amqlib.URI("guest", "guest", "localhost", "5672"))
	wg := &sync.WaitGroup{}
	wg.Add(5)
	// объявляем опции которые хотим применять к Consumer,Queue,Qos
	queConsumer := amqlib.ConsumeOptions{
		Autoack:   false,
		Exclusive: false,
		Nolocal:   false,
		Nowait:    false,
		Arguments: nil,
	}
	queQue := amqlib.QueueOptions{
		Durable:     true,
		Autodeleted: false,
		Exclusive:   false,
		Nowait:      false,
		Arguments:   nil,
	}
	queQos := amqlib.QosOptions{
		PCount: 1,
		PSize:  0,
		Global: false,
	}

	// объявляем два consumer

	consumer1 := amqlib.NewConsumer(amqlib.Queue, "example_que", func(msg amqp091.Delivery) error {

		fmt.Println("msg:c1->", string(msg.Body))
		wg.Done()
		return nil
	}, amqlib.WithConsume(queConsumer), amqlib.WithQueue(queQue), amqlib.WithQos(queQos))
	consumer2 := amqlib.NewConsumer(amqlib.Queue, "example_que", func(msg amqp091.Delivery) error {

		fmt.Println("msg:c2->", string(msg.Body))
		wg.Done()
		return nil
	}, amqlib.WithConsume(queConsumer), amqlib.WithQueue(queQue), amqlib.WithQos(queQos))

	publisher := amqlib.NewPublisher(amqlib.Queue, "example_que", nil, 300, 20, 1)
	onstart := &sync.WaitGroup{}
	onstart.Add(1)
	go func() {
		err := conect.OnTask(ctx, onstart, consumer1, consumer2, publisher)

		if err != nil {
			panic(err)
		}
	}()

	onstart.Wait()
	for i := 0; i < 5; i++ {
		// публикуем данные
		err := publisher.Push(NewStringMsg(fmt.Sprint("hello queue ", i), ""))
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second * 1)
	}
	wg.Wait()

	// Output:
	// msg:c1-> hello queue 0
	// msg:c2-> hello queue 1
	// msg:c1-> hello queue 2
	// msg:c2-> hello queue 3
	// msg:c1-> hello queue 4

}

// Example_pub пример работы с topic реализацией rabbit
func Example_topic() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	// создаем новое подключение к rabbit (rabbit предварительно запущен из example)
	conect := amqlib.NewConnect(5*time.Second, 5, amqlib.URI("guest", "guest", "localhost", "5672"))
	// определяем когда надо прервать тест по waitgroup
	wg := &sync.WaitGroup{}
	wg.Add(5)

	// слушатель с инициализированными данными по умолчанию exchange mn_topic и handler выводящий данные
	// добавляем опцию с топиками
	consumer := amqlib.NewConsumer(amqlib.Topics, "mn_topic", func(msg amqp091.Delivery) error {

		fmt.Println("msg->", string(msg.Body))
		wg.Done()
		return nil
	}, amqlib.WithTopics("my.example.topic"))

	// издатель инициализированный на exchange mn1
	publisher := amqlib.NewPublisher(amqlib.Topics, "mn_topic", nil, 300, 20, 1)
	onstart := &sync.WaitGroup{}
	onstart.Add(1)
	go func() {
		// Осуществляется связь соединения и элементов.
		// ! Слушателя и издателя рекомендовано применять с разными соединениями !
		err := conect.OnTask(ctx, onstart, consumer, publisher)

		if err != nil {
			panic(err)
		}
	}()

	onstart.Wait()
	for i := 0; i < 5; i++ {
		// пример публикации сообщения
		err := publisher.Push(NewStringMsg(fmt.Sprint("hello topic ", i), "my.example.topic"))
		if err != nil {
			panic(err)
		}
	}
	wg.Wait()

	// Output:
	// msg-> hello topic 0
	// msg-> hello topic 1
	// msg-> hello topic 2
	// msg-> hello topic 3
	// msg-> hello topic 4

}
