package amqlib

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	"sync"
)

// Algoritm тип рапределения сообщений которое реализует Membered
type Algoritm int

const (
	// Queue https://www.rabbitmq.com/tutorials/tutorial-two-go.html
	Queue Algoritm = iota
	// PubSub https://www.rabbitmq.com/tutorials/tutorial-three-go.html
	PubSub
	// Routing https://www.rabbitmq.com/tutorials/tutorial-four-go.html
	Routing
	// Topics https://www.rabbitmq.com/tutorials/tutorial-five-go.html
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

// Membered описывает интерфейс участников обмена, которые подключаются к rabbitMQ
type Membered interface {
	// Work деятельность, которую выполняет в рамках подключения
	Work(context.Context, *sync.WaitGroup, *amqp091.Channel) error
}
