package rabbitmq

//Exchange, routing_key(topic), queue

//Exchange:
/*
  Type: direct, topic, fanout, header
*/
type Broker interface {
	Publisher(topic string, reliable bool) (Publisher, error)
	Subscribe(name, topic string, reliable, requeue bool, handler interface{}) error
}

type Publisher interface {
	Publish(m interface{}) error
}

//Publish should identify DeliveryTag and manage it for message Confirm or expire
