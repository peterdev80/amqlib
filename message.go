package amqlib

import (
	"github.com/rabbitmq/amqp091-go"
)

type Published interface {
	Message(interface{}) amqp091.Publishing
}
