# amqlib
Обертка для работы с rabbitMQ

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
publisher := amqlib.NewPublisher("slot_2")

// для остановки сервисов используется контекст
ctx, stop := context.WithCancel(context.Background())
defer stop()

// подключаемся к серверу сообщений и запускаем сервисы
err := conect.OnTask(ctx, consumer, publisher)
if err != nil {
log.Printf("error: %v", err)
}


// публикация сообщений 
	err := publisher.Push(NewStringMsg(3000).Message(fmt.Sprint("hello", time.Now())))
```
