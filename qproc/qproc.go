package qproc

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Publisher struct {
	*amqp.Channel
	exchange string
}

type PayloadProcessor func(*Publisher, *amqp.Delivery) error

func CreateConsumer(conn *amqp.Connection, exchange, exchangeType, queuePattern, consumer string) (<-chan amqp.Delivery, error) {
	ch, err := CreateChannel(conn, exchange, exchangeType)
	if err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	if err = ch.QueueBind(q.Name, queuePattern, exchange, false, nil); err != nil {
		return nil, err
	}
	devs, err := ch.Consume(q.Name, consumer, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return devs, nil
}

func CreatePublisher(conn *amqp.Connection, exchange, exchangeType string) (*Publisher, error) {
	ch, err := CreateChannel(conn, exchange, exchangeType)
	if err != nil {
		return nil, err
	}
	return &Publisher{ch, exchange}, nil
}

func (p *Publisher) PublishMsg(key string, content map[string]interface{}) error {
	b, err := json.Marshal(content)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         b,
	}
	return p.Publish(p.exchange, key, false, false, msg)
}

func CreateChannel(conn *amqp.Connection, exchange, exchangeType string) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = ch.ExchangeDeclare(exchange, exchangeType, true, false, false, false, nil); err != nil {
		return nil, err
	}
	return ch, nil
}

func ProcessQueue(conn *amqp.Connection, pub *Publisher, exchange, exchangeType, queuePattern, consumer string, errCh chan<- error, qprocessor PayloadProcessor) error {
	devs, err := CreateConsumer(conn, exchange, exchangeType, queuePattern, consumer)
	if err != nil {
		return nil
	}
	log.Printf("starting queue %s for %s", exchange, queuePattern)
	go func(devs <-chan amqp.Delivery) {
		for d := range devs {
			log.Println("received delivery")
			err := qprocessor(pub, &d)
			if err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}(devs)
	return nil
}
