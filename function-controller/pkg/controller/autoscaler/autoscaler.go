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

package autoscaler

import (
	"github.com/projectriff/riff/message-transport/pkg/transport/metrics"
	"fmt"
	"sync"
	"time"
	"math"
	"io"
)

//go:generate mockery -name=AutoScaler -output mockautoscaler -outpkg mockautoscaler

type AutoScaler interface {
	// Run starts the autoscaler receiving and sampling metrics.
	Run()

	// Close stops the autoscaler receiving and sampling metrics.
	io.Closer

	// InformFunctionReplicas is used to tell the autoscaler the actual number of replicas there are for a given
	// function. The function is not necessarily being monitored by the autoscaler.
	InformFunctionReplicas(function FunctionId, replicas int)

	// StartMonitoring starts monitoring metrics for the given topic and function.
	StartMonitoring(topic string, function FunctionId) error

	// StopMonitoring stops monitoring metrics for the given topic and function.
	StopMonitoring(topic string, function FunctionId) error

	// Propose proposes the number of replicas for functions that are being monitored.
	Propose() map[FunctionId]int
}

// FunctionId identifies a function in the default namespace.
type FunctionId struct {
	Function string
}

// NewAutoScaler constructs an autoscaler instance using the given metrics receiver, the given required number of scale
// down proposals, and the given sampling interval or a default value if no interval is given.
func NewAutoScaler(metricsReceiver metrics.MetricsReceiver, requiredScaleDownProposals int, samplingInterval ... time.Duration) *autoScaler {
	return &autoScaler{
		mutex:               &sync.Mutex{},
		metricsReceiver:     metricsReceiver,
		samplingInterval:    getSamplingInterval(samplingInterval...),
		totals:              make(map[string]map[FunctionId]*metricsTotals),
		requiredScaleDownProposals: requiredScaleDownProposals,
		proposals:           make(map[FunctionId]*Proposal),
		replicas:            make(map[FunctionId]int),
		stop:                make(chan struct{}),
		accumulatingStopped: make(chan struct{}),
		samplingStopped:     make(chan struct{}),
	}
}

func getSamplingInterval(samplingInterval ... time.Duration) time.Duration {
	var interval time.Duration
	switch len(samplingInterval) {
	case 0:
		interval = time.Hour * 24 * 365 * 100 // turn off automatic sampling //time.Millisecond * 10
	case 1:
		interval = samplingInterval[0]
	default:
		panic("At most one sampling interval may be specified")
	}
	return interval
}

func (a *autoScaler) Run() {
	go a.receiveLoop()
	go a.samplingLoop()
}

type autoScaler struct {
	mutex                      *sync.Mutex
	metricsReceiver            metrics.MetricsReceiver
	samplingInterval           time.Duration
	totals                     map[string]map[FunctionId]*metricsTotals
	requiredScaleDownProposals int
	proposals                  map[FunctionId]*Proposal
	replicas                   map[FunctionId]int // tracks all functions, including those which are not being monitored
	stop                       chan struct{}
	accumulatingStopped        chan struct{}
	samplingStopped            chan struct{}
}

// metrics counts the number of messages transmitted to a Subscription's topic and received by the Subscription.
type metricsTotals struct {
	transmitCount int32
	receiveCount  int32
}

func (a *autoScaler) Propose() map[FunctionId]int {
	a.TakeSample()

	proposals := make(map[FunctionId]int)
	for funcId, proposal := range a.proposals {
		proposals[funcId] = proposal.Get()
	}
	return proposals
}

func (a *autoScaler) StartMonitoring(topic string, function FunctionId) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	funcTotals, ok := a.totals[topic]
	if !ok {
		funcTotals = make(map[FunctionId]*metricsTotals)
		a.totals[topic] = funcTotals
	}

	_, ok = funcTotals[function]
	if ok {
		return fmt.Errorf("Already monitoring topic %s and function %s", topic, function)
	}

	funcTotals[function] = &metricsTotals{}

	a.proposals[function] = NewProposal(a.requiredScaleDownProposals)

	return nil
}

func (a *autoScaler) StopMonitoring(topic string, function FunctionId) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	funcTotals, ok := a.totals[topic]
	if !ok {
		return fmt.Errorf("Not monitoring topic %s and function %s", topic, function)
	}

	_, ok = funcTotals[function]
	if !ok {
		return fmt.Errorf("Not monitoring topic %s and function %s", topic, function)
	}

	delete(funcTotals, function)

	// Avoid leaking memory.
	if len(funcTotals) == 0 {
		delete(a.totals, topic)
	}
	delete(a.proposals, function)

	return nil
}

func (a *autoScaler) receiveLoop() {
	producerMetrics := a.metricsReceiver.ProducerMetrics()
	consumerMetrics := a.metricsReceiver.ConsumerMetrics()
	for {
		select {
		case pm, ok := <-producerMetrics:
			if ok {
				a.receiveProducerMetric(pm)
			}

		case cm, ok := <-consumerMetrics:
			if ok {
				a.receiveConsumerMetric(cm)
			}

		case <-a.stop:
			close(a.accumulatingStopped)
			return
		}
	}
}

func (a *autoScaler) receiveConsumerMetric(cm metrics.ConsumerAggregateMetric) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	funcTotals, ok := a.totals[cm.Topic]
	if ok {
		mt, ok := funcTotals[FunctionId{cm.Function}]
		if ok {
			mt.receiveCount += cm.Count
		}
	}
}

func (a *autoScaler) receiveProducerMetric(pm metrics.ProducerAggregateMetric) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	funcTotals, ok := a.totals[pm.Topic]
	if ok {
		for _, mt := range funcTotals {
			mt.transmitCount += pm.Count
		}
	}
}

// FIXME: delete this function if it is dead code
func (a *autoScaler) samplingLoop() {
	for {
		select {
		case <-time.After(a.samplingInterval):
			a.TakeSample()

		case <-a.stop:
			close(a.samplingStopped)
			return
		}
	}
}

func (a *autoScaler) TakeSample() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	for _, funcTotals := range a.totals {
		for fn, mt := range funcTotals {
			var proposedReplicas int
			if mt.receiveCount == 0 {
				if mt.transmitCount == 0 {
					proposedReplicas = 0
				} else {
					proposedReplicas = 1 // arbitrary value
				}
			} else {
				proposedReplicas = int(math.Floor(float64(a.replicas[fn]) * float64(mt.transmitCount) / float64(mt.receiveCount)))
			}
			a.proposals[fn].Propose(proposedReplicas)
			// FIXME: support fn.Spec.MaxReplicas
			// Zero the sampled metrics for the next interval
			funcTotals[fn] = &metricsTotals{}
		}
	}
}

func (a *autoScaler) InformFunctionReplicas(function FunctionId, replicas int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.replicas[function] = replicas
}

func (a *autoScaler) Close() error {
	close(a.stop)
	<-a.accumulatingStopped
	return nil
}
