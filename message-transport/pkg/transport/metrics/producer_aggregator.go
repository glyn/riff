/*
 * Copyright 2018-Present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metrics

import (
	"github.com/projectriff/riff/message-transport/pkg/transport"
	"github.com/projectriff/riff/message-transport/pkg/message"
	"time"
	"encoding/json"
	"io"
	"log"
)

const producerSource = "producer"

const maxFlushCount = 10
const maxFlushDuration = 100 * time.Millisecond

// ProducerAggregateMetric represents the transmission of a number of messages to a topic by a producer in a time interval.
type ProducerAggregateMetric struct {
	Topic      string
	ProducerId string
	Interval   time.Duration
	Count      int32
}

type producerAggregator struct {
	producerId      string
	metricsTopic    string
	metricsProducer transport.Producer
	counters        map[string]*counter
	sendChan        chan string
	stop            chan struct{}
	errChan         chan error
}

func newProducerAggregator(producerId string, metricsTopic string, metricsProducer transport.Producer) *producerAggregator {
	pa := &producerAggregator{
		producerId:      producerId,
		metricsTopic:    metricsTopic,
		metricsProducer: metricsProducer,
		counters:        make(map[string]*counter),
		sendChan:        make(chan string),
		stop:            make(chan struct{}),
		errChan:         make(chan error),
	}

	go pa.sendAsync()

	return pa
}

type counter struct {
	lastSend time.Time
	count    int32
}

func (pa *producerAggregator) send(topic string) error {
	// If sending to the metrics topic previously failed, return the error, otherwise bump the topic count and possibly
	// send the metrics.
	select {
	case pa.sendChan <- topic:

	case err := <-pa.errChan:
		return err
	}

	return nil
}

func (pa *producerAggregator) sendAsync() {
	for {
		select {
		case topic := <-pa.sendChan:
			now := time.Now()
			c, ok := pa.counters[topic]
			if ok {
				c.count++
			} else {
				c = &counter{
					lastSend: now,
					count:    1,
				}
				pa.counters[topic] = c
			}

			if c.count > maxFlushCount-1 || now.After(c.lastSend.Add(maxFlushDuration)) {
				err := pa.metricsProducer.Send(pa.metricsTopic, pa.createProducerMetricMessage(topic, c.count, now.Sub(c.lastSend)))
				if err != nil {
					log.Printf("Failed to send producer metrics: %v", err)
					pa.errChan <- err
				} else {
					c.count = 0
					c.lastSend = now
				}
			}

		case <-time.After(maxFlushDuration):
			now := time.Now()
			for topic, c := range pa.counters {
				if now.After(c.lastSend.Add(maxFlushDuration)) {
					err := pa.metricsProducer.Send(pa.metricsTopic, pa.createProducerMetricMessage(topic, c.count, now.Sub(c.lastSend)))
					if err != nil {
						log.Printf("Failed to send producer metrics: %v", err)
						pa.errChan <- err
					} else {
						c.count = 0
						c.lastSend = now
					}
				}
			}

		case <-pa.stop:
			return
		}
	}
}

func (pa *producerAggregator) createProducerMetricMessage(topic string, count int32, interval time.Duration) message.Message {
	metric, err := json.Marshal(ProducerAggregateMetric{
		Topic:      topic,
		ProducerId: pa.producerId,
		Interval:   interval,
		Count:      count,
	})
	if err != nil { // should never happen
		panic(err)
	}
	return message.NewMessage(metric, message.Headers{"source": []string{producerSource}})
}

func (pa *producerAggregator) Close() error {
	close(pa.stop)
	var err error = nil
	if metricsProducer, ok := pa.metricsProducer.(io.Closer); ok {
		err = metricsProducer.Close()
	}
	return err
}
