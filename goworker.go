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

	// create a publisher
	/*
	pub, err := createPublisher(mdConn, "metricEvents", "fanout")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	*/

	devs, err := createConsumer(mdConn, "metrics", "topic", "metrics.*", "")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	
	log.Printf("begin consuming")
	done := make(chan error, 1)

	go func() {
		log.Printf("in goroutine")
		for d := range devs {
			fmt.Printf("Got us a queue item: %d B, [%v], %q :: %+v\n", len(d.Body), d.DeliveryTag, d.Body, d)
		}
		done <- nil
	}()

	// a queue for metric results
	res, err := createConsumer(mdConn, "metricResults", "x-consistent-hash", "10", "")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	resdone := make(chan error, 1)
	go func() {
		log.Printf("in results goroutine")
		for d := range res {
			fmt.Printf("Got us a results queue item: %d B, [%v], %q :: %+v\n", len(d.Body), d.DeliveryTag, d.Body, d)
		}
		resdone <- nil
	}()
	
	<- done
	<- resdone
	fmt.Println("all done!")
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
