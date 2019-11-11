package rabbitmq

import "github.com/streadway/amqp"

type subscriber struct {
	reliable bool
	name     string
	url      string
	topic    string
	*amqp.Connection
}

func newSubscriber(reliable bool, name, url, topic string, conn *amqp.Connection) *subscriber {
	if conn == nil {
		var err error
		conn, err = amqp.Dial(url)
		if err != nil {
			panic("failed to new connection to AMQP.")
			return nil
		}
	}
	return &subscriber{reliable, name, url, topic, conn}
}

func (s *subscriber) Consume() (<-chan amqp.Delivery, error) {
	channel, err := s.Channel()
	if err != nil {
		return nil, err
	}
	//Exchange Declare
	err = channel.ExchangeDeclare(
		s.name,
		defaultExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	args := amqp.Table{}
	if !s.reliable {
		args["x-message-ttl"] = 20 * 1000
	}
	_, err = channel.QueueDeclare(
		s.name,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return nil, err
	}
	//queue, key, exchange, false, nil
	err = channel.QueueBind(
		s.name,
		s.topic,
		defaultExchange,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	//queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table
	return channel.Consume(s.name, "", !s.reliable, false, false, false, nil)
}
