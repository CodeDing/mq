package main

import (
	"fmt"
	"mq/proto"

	"log"
	"time"

	"github.com/CodeDing/mq/rabbitmq"
)

var (
	topic            = "test.topic.hello"
	topic2           = "test.topic.message"
	ankrBroker       rabbitmq.Broker

	helloPublisher   rabbitmq.Publisher
	helloSubscriber1 = logHandler{name: "hello1"}

	messagePublisher  rabbitmq.Publisher
	messageSubscriber = messageHandler{ID: 100000}

	PUBLISH_INTERVAL = 2 * time.Second
)

type logHandler struct {
	name string
}

func (s *logHandler) handle(h *proto.Hello) error {
	log.Printf("[%s] consume %+v", s.name, h)
	return nil
}

type messageHandler struct {
	ID uint64
}

func (m *messageHandler) handle(h *proto.Hello) error {
	log.Printf("ID:[%d] consume %+v", m.ID, h)
	return nil
}

func init() {
	var err error
	ankrBroker = rabbitmq.NewBroker()
	if helloPublisher, err = ankrBroker.Publisher(topic, true); err != nil {
		log.Fatal(err)
	}
	if err = ankrBroker.Subscribe("hello1", topic, true, false, helloSubscriber1.handle); err != nil {
		log.Fatal(err)
	}

	//if messagePublisher, err = ankrBroker.Publisher(topic2, false); err != nil {
	//	log.Fatal(err)
	//}
	//if err = ankrBroker.Subscribe("message", topic2, true, false, messageSubscriber.handle); err != nil{
	//	log.Fatal(err)
	//}
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

		//if err := messagePublisher.Publish(&msg); err != nil {
		//	log.Printf("[pub(message)] failed: %v", err)
		//
		//} else {
		//	log.Printf("[pub(message)] pubbed message: %v", msg)
		//}
		i++
	}
}

func main() {
	time.Sleep(1 * time.Second)
	go pub()
	<-time.After(time.Second * 600)
}
