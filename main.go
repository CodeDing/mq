package main

import (
	"fmt"
	"mq/proto"

	"log"
	"time"

	"github.com/Ankr-network/dccn-common/broker/rabbitmq"

	"github.com/Ankr-network/dccn-common/broker"
)

var (
	topic            = "ankr.topic.hello"
	ankrBroker       broker.Broker
	helloPublisher   broker.Publisher
	helloSubscriber1 = logHandler{name: "hello1"}

	PUBLISH_INTERVAL = 5 * time.Second
)

type logHandler struct {
	name string
}

func (s *logHandler) handle(h *proto.Hello) error {
	log.Printf("[%s] handle %+v", s.name, h)
	return nil
}

func init() {
	var err error
	ankrBroker = rabbitmq.NewBroker()
	if helloPublisher, err = ankrBroker.Publisher(topic, true); err != nil {
		log.Fatal(err)
	}
	if err := ankrBroker.Subscribe("hello1", topic, true, false, helloSubscriber1.handle); err != nil {
		log.Fatal(err)
	}
}

func pub() {
	tick := time.NewTicker(PUBLISH_INTERVAL)
	i := 0
	for range tick.C {
		msg := proto.Hello{Name: fmt.Sprintf("No.%d", i)}
		if err := helloPublisher.Publish(&msg); err != nil {
			log.Printf("[pub] failed: %v", err)
		} else {
			log.Printf("[pub] pubbed message: %v", msg.Name)
		}
		i++
	}
}

func main() {
	time.Sleep(1 * time.Second)
	go pub()
	<-time.After(time.Second * 100)
}
