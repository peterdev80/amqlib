package amqlib

import "github.com/rabbitmq/amqp091-go"

// ExchangeOptions опции которые необходимо установить exchange
type ExchangeOptions struct {
	Durable     bool          // Не удаляемый при рестарте exchange
	Autodeleted bool          // Временный удаляемый при отсутствии привязки exchange
	Internal    bool          // Внутренний exchange, скрытый от внешних брокеров
	Nowait      bool          // Не дожидаться подтверждения
	Arguments   amqp091.Table // Расширенные параметры
}

// ConsumeOptions которые будут применены к Consume
type ConsumeOptions struct {
	Autoack   bool // Сервер вернет true до отправки в сеть, при true нет необходимости подтверждать получение
	Exclusive bool // При true больше потребителей нет, иначе сервер распределяет всем
	Nolocal   bool
	Nowait    bool // Не надо ждать подтверждения от сервера
	Arguments amqp091.Table
}

// QueueOptions опции очереди
type QueueOptions struct {
	Durable     bool // Не удаляемый при рестарте exchange
	Autodeleted bool // Временный удаляемый при отсутствии привязки exchange
	Exclusive   bool // При true больше потребителей нет, иначе сервер распределяет всем
	Nowait      bool // Не надо ждать подтверждения от сервера
	Arguments   amqp091.Table
}

// QosOptions принцип распределения работы между слушателями
type QosOptions struct {
	PCount int
	PSize  int
	Global bool
}

type options struct {
	eopt   ExchangeOptions // Опции для exchange, если nil то используются по умолчанию
	copt   ConsumeOptions  // Опции для consume
	qopt   QueueOptions    // Опции для очереди
	topics []string        // Список поддерживаемых тем
	qos    *QosOptions
}

// Option интерфейс опций
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

// WithExchange изменить установку по умолчанию для exchange
func WithExchange(eopt ExchangeOptions) Option {
	return optionFunc(func(o *options) {
		o.eopt = eopt
	})
}

// WithConsume изменить установку по умолчанию для consume
func WithConsume(copt ConsumeOptions) Option {
	return optionFunc(func(o *options) {
		o.copt = copt
	})
}

// WithTopics топики для прослушивания
func WithTopics(topics ...string) Option {
	return optionFunc(func(o *options) {
		o.topics = topics
	})
}

// WithQueue изменить установку по умолчанию для очереди
func WithQueue(qopt QueueOptions) Option {
	return optionFunc(func(o *options) {
		o.qopt = qopt
	})
}

// WithQos опции распределять нагрузку
func WithQos(qos QosOptions) Option {
	return optionFunc(func(o *options) {
		o.qos = &qos
	})
}
