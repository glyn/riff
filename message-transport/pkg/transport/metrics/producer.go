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
	"io"
)

// NewProducer decorates the given delegate to send producer metrics for the given producer id to the given topic using the
// given metrics producer. The given producer id can be any unique identifier for the producer and can be pod
// instance-specific (that is, it need not carry across when a pod is restarted).
func NewProducer(delegate transport.Producer, producerId string, metricsTopic string, metricsProducer transport.Producer) *producer {
	return &producer{
		delegate: delegate,
		aggregator: &producerAggregator{
			producerId:      producerId,
			metricsTopic:    metricsTopic,
			metricsProducer: metricsProducer,
		},
	}
}

type producer struct {
	delegate   transport.Producer
	aggregator *producerAggregator
}

func (p *producer) Send(topic string, msg message.Message) error {
	err := p.delegate.Send(topic, msg)
	if err == nil {
		metricsErr := p.aggregator.send(topic)
		if metricsErr != nil {
			return metricsErr
		}
	}
	return err
}

func (p *producer) Errors() <-chan error {
	return p.delegate.Errors()
}

func (p *producer) Close() error {
	var err error = nil
	if delegate, ok := p.delegate.(io.Closer); ok {
		err = delegate.Close()
	}

	err2 := p.aggregator.Close()
	if err != nil {
		return err
	}
	return err2
}
