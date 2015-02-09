package main

/*
Some important libs that have turned up - may or may not be in this file:
https://github.com/streadway/amqp -- rabbitmq
https://github.com/mattbaird/elastigo -- elasticsearch
https://github.com/marpaia/graphite-golang -- carbon
*/
import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
)

type publisher struct {
	*amqp.Channel
}

type PayloadProcessor func(*publisher, *amqp.Delivery) error

// dev var declarations, until real config/flags are added
var rabbitURL string = "amqp://rabbitmq"

func main() {
	// First fire up a queue to consume metric def events
	mdConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer mdConn.Close()
	log.Println("connected")

	done := make(chan error, 1)
	
	// create a publisher
	pub, err := createPublisher(mdConn, "metricEvents", "fanout")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	testProc := func(pub *publisher, d *amqp.Delivery) error {
		fmt.Printf("Got us a queue item: %d B, [%v], %q :: %+v\n", len(d.Body), d.DeliveryTag, d.Body, d)
		return nil
	}

	err = ProcessQueue(mdConn, nil, "metrics", "topic", "metrics.*", "", done, testProc)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	err = ProcessQueue(mdConn, pub, "metricResults", "x-consistent-hash", "10", "", done, testProc)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = <- done
	fmt.Println("all done!")
	if err != nil {
		log.Printf("Had an error, aiiieeee! '%s'", err.Error())
	}
}

func createConsumer(conn *amqp.Connection, exchange, exchangeType, queuePattern, consumer string) (<-chan amqp.Delivery, error) {
	ch, err := createChannel(conn, exchange, exchangeType)
	if err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("queue declared")
	if err = ch.QueueBind(q.Name, queuePattern, exchange, false, nil); err != nil {
		return nil, err
	}
	log.Printf("queue bound")
	devs, err := ch.Consume(q.Name, consumer, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return devs, nil
}

func createPublisher(conn *amqp.Connection, exchange, exchangeType string) (*publisher, error) {
	ch, err := createChannel(conn, exchange, exchangeType)
	if err != nil {
		return nil, err
	}
	return &publisher{ ch }, nil
}

func createChannel(conn *amqp.Connection, exchange, exchangeType string) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	
	log.Printf("channel created")
	if err = ch.ExchangeDeclare(exchange, exchangeType, false, false, false, false, nil); err != nil {
		return nil, err
	}
	log.Printf("exchange declared")
	return ch, nil
}

func ProcessQueue(conn *amqp.Connection, pub *publisher, exchange, exchangeType, queuePattern, consumer string, errCh chan<- error, qproc PayloadProcessor) error {
	devs, err := createConsumer(conn, exchange, exchangeType, queuePattern, consumer)
	if err != nil {
		return nil
	}
	go func(devs <-chan amqp.Delivery) {
		for d := range devs {
			err := qproc(pub, &d)
			if err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}(devs)
	return nil
}
