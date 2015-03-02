/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//Package qproc has functions to simplify processing metrics queues.
package qproc

import (
	"encoding/json"
	"errors"
	"github.com/ctdk/goas/v2/logger"
	"github.com/streadway/amqp"
	"time"
)

// Publisher is a rabbitmq channel configured for a processing function to
// publish results.
type Publisher struct {
	*amqp.Channel
	exchange string
}

// PayloadProcessor is a function type that can be passed as an argument to
// ProcessQueue to process deliveries from rabbitmq.
type PayloadProcessor func(*Publisher, *amqp.Delivery) error

// CreateConsumer creates a consumer queue.
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

// CreatePublisher creates a publishing queue.
func CreatePublisher(conn *amqp.Connection, exchange, exchangeType string) (*Publisher, error) {
	ch, err := CreateChannel(conn, exchange, exchangeType)
	if err != nil {
		return nil, err
	}
	return &Publisher{ch, exchange}, nil
}

// PublishMsg publishes a message to the rabbitmq queue it was configured with
// when it was set up.
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

// CreateChannel is a wrapper to simplify creating a channel with its attendant
// exchange.
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

// ProcessQueue creates a consumer queue on the given connection using the
// supplied exchange, exchange type, queue pattern, and number of workers. A
// function that satisfies the PayloadProcessor function type is also passed in
// to process the jobs off the queue, and if the payload processor should
// publish the results of its work a Publisher may be supplied. Otherwise nil
// for the Publisher is fine.
func ProcessQueue(conn *amqp.Connection, pub *Publisher, exchange, exchangeType, queuePattern, consumer string, errCh chan<- error, qprocessor PayloadProcessor, numWorkers int) error {
	if numWorkers < 1 {
		err := errors.New("numWorkers must be at least 1")
		return err
	}
	for i := 0; i < numWorkers; i++ {
		devs, err := CreateConsumer(conn, exchange, exchangeType, queuePattern, consumer)
		if err != nil {
			return nil
		}
		logger.Infof("starting queue %s for %s", exchange, queuePattern)
		go func(i int, exchange string, devs <-chan amqp.Delivery) {
			for d := range devs {
				logger.Debugf("worker %s %d received delivery", exchange, i)
				err := qprocessor(pub, &d)
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(i, exchange, devs)
	}
	return nil
}
