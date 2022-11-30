package amqlib

import (
	"github.com/rabbitmq/amqp091-go"
)

// Published интерфейс структуры которая работает с amqllib
// генерация сообщения для передачи в rabbit
type Published interface {
	Message() amqp091.Publishing
	Key() string
}
