package rabbitmq

import (
	"github.com/streadway/amqp"
	"regexp"
)

const (
	defaultExchange = "ankr.micro"
	rabbitURLRegx   = regexp.MustCompile("^amqp(s)?://.*")
)

type rabbitBroker struct {
	url string
	*amqp.Connection
}

func NewBroker(url string) Broker {
	if !rabbitURLRegx.MatchString(url) {
		return nil
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil
	}
	return &rabbitBroker{url, conn}
}
/*
1. connection
2. channel over connection
3. exchange declare
4. queue declare
5. bind exchange, queue
*/
func (r *rabbitBroker) channel() (*amqp.Channel, error) {
	return r.Channel()
}

func (r *rabbitBroker) Publisher(topic string, reliable bool) (Publisher, error) {
	return nil,nil
}

func (r *rabbitBroker) Subscribe(name, topic string, reliable, requeue bool, handler interface{}) error {
	return nil
}
