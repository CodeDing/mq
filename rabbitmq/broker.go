package rabbitmq

import (
	"fmt"
	"regexp"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/streadway/amqp"
)

const (
	defaultExchange     = "ankr.micro"
	defaultExchangeType = "topic"
	defaultRabbitmqURL  = "amqp://guest:guest@localhost:5672"
)

var (
	rabbitURLRegx     = regexp.MustCompile(`^amqp(s)?://.*`)
	defaultRetryDelay = time.Millisecond * 200
	reconnectDelay    = time.Second * 10
)

type rabbitBroker struct {
	url  string
	conn *amqp.Connection
}

func NewBroker(url ...string) Broker {
	var host string
	if len(url) == 0 {
		host = defaultRabbitmqURL
	} else {
		host = url[0]
	}

	if !rabbitURLRegx.MatchString(host) {
		return nil
	}
	conn, err := amqp.Dial(host)
	if err != nil {
		return nil
	}
	return &rabbitBroker{host, conn}
}

/*
1. connection
2. channel over connection
3. exchange declare
4. queue declare
5. bind exchange, queue
*/
func (r *rabbitBroker) Publisher(topic string, reliable bool) (Publisher, error) {
	p := newPublisher(reliable, r.url, topic, r.conn)
	return p, nil
}

//(reliable bool, name, url, topic string, conn *amqp.Connection)
func (r *rabbitBroker) Subscribe(name, topic string, reliable, requeue bool, handler interface{}) error {
	h, err := newHandler(handler)
	if err != nil {
		return err
	}
	s := newSubscriber(reliable, name, r.url, topic, r.conn)
	delivery, err := s.Consume()
	if err != nil {
		return err
	}

	go func() {
		for d := range delivery {
			var err error
			msg := h.newMessage()
			if err = proto.Unmarshal(d.Body, msg); err != nil {
				fmt.Printf("failed to unmarshal body, err: %+v", err)
				continue
			}
			err = h.call(msg)
			if err != nil {
				fmt.Printf("failed to handler msg, msg: %+v, err: %+v", msg, err)
				err = d.Nack(false, requeue)
				if err != nil {
					fmt.Printf("failed to Nack, err: %+v", err)
				}
				continue
			}
			if s.reliable {
				err = d.Ack(false)
				if err != nil {
					fmt.Printf("failed to Ack, err: %+v", err)
				}
			}

		}
	}()
	return nil
}
