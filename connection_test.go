package amqlib

import (
	"context"
	"github.com/ory/dockertest/v3"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
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

func TestMainSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(MainSuite))
}

type MainSuite struct {
	suite.Suite
	resource *dockertest.Resource
	wg       sync.WaitGroup
}

func (ms *MainSuite) SetupSuite() {

	ms.wg.Add(1)
	defer ms.wg.Done()
	pool, err := dockertest.NewPool("")
	pool.MaxWait = time.Minute * 2
	if err != nil {
		log.Fatal().Msgf("Could not connect to docker: %s", err)
	}
	resource, err := pool.Run("rabbitmq", "3-management",
		[]string{"RABBITMQ_DEFAULT_USER=user", "RABBITMQ_DEFAULT_PASS=user"})
	if err != nil {
		panic(err)
	}
	ms.resource = resource
	step := 0
	for {
		if step == 60 {
			panic("many fail connect")
		}
		conn, err := amqp091.Dial(string(URI("user", "user", "localhost", ms.resource.GetPort("5672/tcp")))) // подключаемся к серверу
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(time.Second)
		step++
	}

}

func (ms *MainSuite) TestConnect_OnTask() {
	rl := URI("user", "user", "localhost", ms.resource.GetPort("5672/tcp"))
	ctxcancel, cancel := context.WithCancel(context.Background())
	defer cancel()
	type fields struct {
		reconnectTime time.Duration
		maxIteration  int
		addr          Addr
	}
	type args struct {
		ctx     context.Context
		members []Membered
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{name: "Correct cancel",
			fields:  fields{reconnectTime: time.Second, maxIteration: 5, addr: rl},
			args:    args{ctx: ctxcancel, members: []Membered{NewPublisher(1, "qwerty", nil, 10, 10, 1)}},
			wantErr: false,
		},
	}
	for _, tt := range tests {

		c := NewConnect(tt.fields.reconnectTime, tt.fields.maxIteration, tt.fields.addr)
		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {

			err := c.OnTask(tt.args.ctx, &wg, tt.args.members...)
			ms.Assert().NoError(err, "корректное завершение работы")

		}()
		wg.Wait()
		time.Sleep(time.Second)
		switch tt.name {
		case "Correct cancel":
			cancel()
		default:

		}

	}
	time.Sleep(time.Second)
}

func (ms *MainSuite) TearDownSuite() {

	if err := ms.resource.Close(); err != nil {
		log.Fatal().Err(err).Msg("error removing container")
	}
}
