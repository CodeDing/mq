package rabbitmq

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
)

//https://gist.github.com/OhBonsai/28868448ba84c38749a55ea63f22ca77

type publisher struct {
	reliable    bool
	url         string
	topic       string
	isConnected bool
	done        chan struct{}
	conn        *amqp.Connection
}

func newPublisher(reliable bool, url, topic string, conn *amqp.Connection) *publisher {
	if conn == nil {
		var err error
		conn, err = amqp.Dial(url)
		if err != nil {
			panic("failed to new connection to AMQP.")
		}
	}
	done := make(chan struct{})
	p := &publisher{reliable, url, topic, true, done, conn}
	//go p.reconnect()
	return p
}

func (p *publisher) reconnect() {
	for {
		for !p.conn.IsClosed() {
			fmt.Printf("Publisher connection is not closed. time: %s\n", time.Now().Format(time.RFC3339))
			time.Sleep(reconnectDelay)
			continue
		}
		p.isConnected = false
		fmt.Println("Publisher is attempting reconnect ...")
		for !p.connect() {
			fmt.Println("Publisher failed to connect ...")
			time.Sleep(reconnectDelay)
		}
		select {
		case <-p.done:
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (p *publisher) connect() bool {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return false
	}
	channel, err := conn.Channel()
	if err != nil {
		return false
	}
	err = channel.Confirm(false)
	if err != nil {
		return false
	}
	err = channel.Close()
	if err != nil {
		return false
	}
	p.conn = conn
	p.isConnected = true
	return true
}

func (p *publisher) Publisher() error {
	if !p.isConnected {
		return ErrPublisherConn
	}
	channel, err := p.conn.Channel()
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
	if !p.isConnected {
		return ErrPublisherConn
	}
	channel, err := p.conn.Channel()
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
		Headers:         amqp.Table{},
		ContentType:     "application/protobuf",
		ContentEncoding: "",
		Body:            body,
		DeliveryMode:    amqp.Transient,
		Priority:        0,
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
		ticker := time.NewTicker(defaultRetryDelay)
		select {
		case confirm := <-confirmCh:
			if confirm.Ack {
				fmt.Printf("Publish message(reliable): msg=> %+v, DeliveryTag=>%d\n", m, confirm.DeliveryTag)
				return nil
			}
		case <-ticker.C:
			fmt.Printf("Publish message(reliable) timeout: msg=> %+v\n", m)
		}
		ticker.Stop()
		fmt.Printf("Publish message(reliable): send message failed\n")
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

func (p *publisher) Close() error {
	if !p.isConnected {
		return ErrPublishConnClose
	}
	err := p.conn.Close()
	if err != nil {
		return err
	}
	close(p.done)
	p.isConnected = false
	return nil
}
