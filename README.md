# amqlib
Обертка для работы с rabbitMQ реализует алгоритмы представленные в https://www.rabbitmq.com/getstarted.html
В библиотеки вводятся сущности реализованные поверх github.com/rabbitmq/amqp091-go, как:
* Connect - подключение а серверу rabbit, реализация и диспетчеризация каналов rabbit. Работа сущности начинается
после вызова OnTask метода.
* Consumer и Publisher - подписчик и издатель. Настраиваются при инициализации
* Published - интерфейс которому должно соответствовать сообщение публикуемое через библиотеку


Пример использования

``` go
// формируем  подключения к серверу rabbitMQ
// 5 сек -задержка перед повторным соединением
// 5 - максимальное количество попыток соединения
// addr - строка подключения
conect := amqlib.NewConnect(5*time.Second, 5,
 amqlib.URI("guest", "guest", "localhost", "5672"))

// описываем обработчик входящих сообщений
handler := func(msg amqp091.Delivery) error {
log.Printf("msg: %s", msg.Body)

return nil
}

// инициализируем новый обработчик сообщений, указывая в качестве параметров
// название точки обмена (Exchange), функцию для обработки сообщений и список
// тем, на которые осуществляется подписка
consumer := amqlib.NewConsumer("slot_1", handler, "msg_1")

// инициализируем сервис публикации сообщений, указав в качестве параметров
// название точки обмена (Exchange)
publisher := amqlib.NewPublisher(Алгоритм работы брокера, "exchange или очередь", опции exchange отличные от умолчания,
ttl параметр для сообщения, время в секундах на отправку сообщений, 1-размер буфера до отправки в rabbit))

// для остановки сервисов используется контекст
ctx, stop := context.WithCancel(context.Background())
defer stop()

// подключаемся к серверу сообщений и запускаем сервисы, слушатель и издатель желательно вешать га разные подключения
err := conect.OnTask(ctx, consumer, publisher)
if err != nil {
log.Printf("error: %v", err)
}


// публикация сообщений 
	err := publisher.Push(сообщене структуры )
```


Опции по умолчанию:
``` go
eopt: ExchangeOptions{
Durable:     true,
Autodeleted: false,
Internal:    false,
Nowait:      false,
Arguments:   nil,
},
copt: ConsumeOptions{
Autoack:   true,
Exclusive: false,
Nolocal:   false,
Nowait:    false,
Arguments: nil,
},
qopt: QueueOptions{
Durable:     false,
Autodeleted: false,
Exclusive:   true,
Nowait:      false,
Arguments:   nil,
},
topics: nil,
qos:    nil,
}
```


# TO-DO
Реализовать асинхронный RPC сервер в рамках библиотеки