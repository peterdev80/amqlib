package amqlib

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
)

// Membered описывает интерфейс участников обмена, которые подключаются к rabbitMQ
type Membered interface {
	// Work деятельность, которую выполняет в рамках подключения
	Work(context.Context, *amqp091.Channel) error
}
