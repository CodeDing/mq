package rabbitmq

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
)

//https://gist.github.com/OhBonsai/28868448ba84c38749a55ea63f22ca77

const (
	reconnectDelay = 10 * time.Second
	resendDelay    = 5 * time.Second
	resendTime     = 3
)

type publisher struct {
	reliable    bool
	url         string
	topic       string
	isConnected bool
	done        chan struct{}
	conn        *amqp.Connection
	notifyClose chan *amqp.Error
}

func newPublisher(reliable bool, url, topic string) *publisher {
	p := &publisher{
		reliable:    reliable,
		url:         url,
		topic:       topic,
		isConnected: false,
		done:        make(chan struct{}),
	}
	go p.handleConnect()
	return p
}

func (p *publisher) handleConnect() {
	for {
		p.isConnected = false
		logger.Printf("Publisher[%s] is attempting connect ...", p.topic)
		for !p.connect() {
			logger.Printf("Publisher[%s] failed to connect.", p.topic)
			time.Sleep(reconnectDelay)
		}
		logger.Printf("Publisher[%s] connected!", p.topic)
		select {
		case <-p.done:
			return
		case <-p.notifyClose:
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
		return false
	}

	err = channel.Close()
	if err != nil {
		return false
	}

	p.conn = conn
	p.notifyClose = make(chan *amqp.Error)
	p.conn.NotifyClose(p.notifyClose)
	p.isConnected = true
	return true
}

func (p *publisher) Publish(m interface{}) error {
	if !p.isConnected {
		return ErrPublisherConn
	}

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

	channel, err := p.conn.Channel()
	if err != nil {
		return ErrCreatePublishChannel
	}

	if p.reliable {
		currentTime := 0
		err := channel.Confirm(false)
		if err != nil {
			return ErrConfirmPublishChannel
		}
		notifyConfirm := make(chan amqp.Confirmation)

		for {
			publishing.DeliveryMode = amqp.Persistent
			if err := channel.Publish(
				defaultExchange,
				p.topic,
				false,
				false,
				publishing,
			); err != nil {
				currentTime += 1
				logger.Printf("Publisher[%s] message(reliable) failed, retry %d time", p.topic, currentTime)
				if currentTime < resendTime {
					time.Sleep(resendDelay)
					continue
				}
				return err
			}
			ticker := time.NewTicker(resendDelay)
			select {
			case confirm := <-channel.NotifyPublish(notifyConfirm):
				if confirm.Ack {
					return nil
				}
			case <-ticker.C:
			}
			ticker.Stop()
			logger.Printf("Publish[%s] message(reliable): send message failed", p.topic)
			return ErrPublishTimeout
		}
	}
	err = channel.Publish(
		defaultExchange,
		p.topic,
		false,
		false,
		publishing,
	)
	logger.Printf("Publish[%s] mesage(unreliable): %+v", p.topic, m)
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
