package rabbitmq

import (
	"fmt"
	"github.com/gogo/protobuf/proto"

	"github.com/streadway/amqp"
)

type publisher struct {
	reliable bool
	url      string
	topic    string
	*amqp.Connection
}

func newPublisher(reliable bool, url, topic string, conn *amqp.Connection) *publisher {
	if conn == nil {
		var err error
		conn, err = amqp.Dial(url)
		if err != nil {
			panic("failed to new connection to AMQP.")
		}
	}
	return &publisher{reliable, url, topic, conn}
}

func (p *publisher) Publisher() error {
	channel, err := p.Channel()
	if err != nil {
		return err
	}
	defer func() {
		if err := channel.Close(); err != nil {
			fmt.Printf("failed to Channel.Close, err: %+v", err)
		}
	}()
	//name, kind string, durable, autoDelete, internal, noWait bool, args Table
	err = channel.ExchangeDeclare(
		defaultExchange,
		defaultExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

func (p *publisher) Publish(m interface{}) error {
	channel, err := p.Channel()
	if err != nil {
		return err
	}

	defer func() {
		if err := channel.Close(); err != nil {
			fmt.Printf("failed to channel.Close, err: %+v", err)
		}
	}()

	msg, ok := m.(proto.Message)
	if !ok {
		return ErrMessageIsNotProtoMessage
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return ErrProtoMarshal
	}

	publishing := amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        body,
	}

	if p.reliable {
		if err := channel.Confirm(false); err != nil {
			return ErrChannelConfirm
		}
		confirmCh := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		publishing.DeliveryMode = amqp.Persistent
		if err := channel.Publish(
			defaultExchange,
			p.topic,
			false,
			false,
			publishing,
		); err != nil {
			return err
		}

		confirm := <-confirmCh
		if !confirm.Ack {
			return ErrPublishMessageNotAck
		}
		fmt.Printf("Publish message(reliable): %+v\n", m)
		return nil
	}
	err = channel.Publish(
		defaultExchange,
		p.topic,
		false,
		false,
		publishing,
	)
	fmt.Printf("Publish mesage(unreliable): %+v\n", m)
	return err
}
