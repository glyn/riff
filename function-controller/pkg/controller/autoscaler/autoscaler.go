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
	"log"
	"io"
	"github.com/projectriff/riff/message-transport/pkg/transport"
)

//go:generate mockery -name=AutoScaler -output mockautoscaler -outpkg mockautoscaler

type AutoScaler interface {
	// Set maximum replica count policy.
	SetMaxReplicasPolicy(func(function FunctionId) int)

	// Set delay scale down policy.
	SetDelayScaleDownPolicy(func(function FunctionId) time.Duration)

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

// FunctionId identifies a function
// TODO: support namespaces.
type FunctionId struct {
	Function string
}

// Go does not provide a MaxInt, so we have to calculate it.
const MaxUint = ^uint(0)
const MaxInt = int(MaxUint >> 1)

// PID controller parameters
const (
	innerInitialSetpoint = queueRateOfChange(0) // somewhat arbitrary value
	innerKp              = 0
	innerKi              = 0
	innerKd              = 0

	outerSetpoint     = queueLength(5)
	k                 = 0.0001
	outerKp           = 0.5 * k  // 0.05 // 0.1 // 1
	outerKi           = 0.05 * k // 0.0005
	outerKd           = 0.05 * k // increasing this above 0 doesn't help the simulated workload because the replica initialisation delay tends to make the autoscaler too aggressive
	integratorPreload = 0        // 0 to switch off
)

// Output filter parameters
const (
	// smoothing
	//linearSmootherGreed = 1 // use 1 to turn off
	linearSmootherGreed      = 1     // use 1 to turn off
	exponentialSmootherGreed = 0.005 // use 1 to turn off
	//exponentialSmootherGreed = 0.05 // use 1 to turn off
	smoothIncreases = false

	// stepping
	stepSize = 1 // use 1 to turn off stepping
)

type queueLength int64
type queueRateOfChange int64

// NewAutoScaler constructs an autoscaler instance using the given metrics receiver and the given transport inspector.
func NewAutoScaler(metricsReceiver metrics.MetricsReceiver, transportInspector transport.Inspector) *autoScaler {
	return &autoScaler{
		mutex:               &sync.Mutex{},
		metricsReceiver:     metricsReceiver,
		transportInspector:  transportInspector,
		totals:              make(map[string]map[FunctionId]*metricsTotals),
		scalers:             make(map[FunctionId]scaler),
		replicas:            make(map[FunctionId]int),
		maxReplicas:         func(function FunctionId) int { return MaxInt },
		delayScaleDown:      func(function FunctionId) time.Duration { return time.Duration(0) },
		stop:                make(chan struct{}),
		accumulatingStopped: make(chan struct{}),
	}
}

func (a *autoScaler) SetMaxReplicasPolicy(maxReplicas func(function FunctionId) int) {
	a.maxReplicas = maxReplicas
}

func (a *autoScaler) SetDelayScaleDownPolicy(delayScaleDown func(function FunctionId) time.Duration) {
	a.delayScaleDown = delayScaleDown
}

func (a *autoScaler) Run() {
	a.mutex.Lock() // fail Run if a.mutex is nil
	defer a.mutex.Unlock()

	go a.receiveLoop()
}

type autoScaler struct {
	mutex               *sync.Mutex // nil when autoScaler is closed
	metricsReceiver     metrics.MetricsReceiver
	transportInspector  transport.Inspector
	totals              map[string]map[FunctionId]*metricsTotals
	scalers             map[FunctionId]scaler
	replicas            map[FunctionId]int // tracks all functions, including those which are not being monitored
	maxReplicas         func(function FunctionId) int
	delayScaleDown      func(function FunctionId) time.Duration
	stop                chan struct{}
	accumulatingStopped chan struct{}
}

// metrics counts the number of messages transmitted to a Subscription's topic and received by the Subscription.
type metricsTotals struct {
	transmitCount int32
	receiveCount  int32
}

func (a *autoScaler) Propose() map[FunctionId]int {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	proposals := make(map[FunctionId]int)
	for _, funcTotals := range a.totals {
		for fn, mt := range funcTotals {
			if _, ok := proposals[fn]; ok {
				panic("Functions with multiple input topics are not supported")
			}
			proposals[fn] = a.scalers[fn](mt)
			// proposals[fn] = max(proposals[fn], a.scalers[fn](mt)) might help multiple input topics

			// Zero the sampled metrics for the next interval
			funcTotals[fn] = &metricsTotals{}
		}
	}

	return proposals
}

func (a *autoScaler) emptyQueue(funcId FunctionId) (bool, int64) {
	for topic, funcTotals := range a.totals {
		if _, ok := funcTotals[funcId]; ok {
			queueLen, err := a.transportInspector.QueueLength(topic, funcId.Function)
			if err != nil {
				log.Printf("Failed to obtain queue length (and will assume it is positive): %v", err)
				return false, -1
			}
			if queueLen > 0 {
				return false, queueLen
			}
		}
	}
	return true, 0
}

func (a *autoScaler) queueLength(funcId FunctionId) queueLength {
	ql := queueLength(0)
	for topic, funcTotals := range a.totals {
		if _, ok := funcTotals[funcId]; ok {
			queueLen, err := a.transportInspector.QueueLength(topic, funcId.Function)
			if err != nil {
				log.Printf("Failed to obtain queue length (and will assume it is positive): %v", err)
				ql++
			} else {
				ql += queueLength(queueLen)
			}
		}
	}
	return ql
}

func (a *autoScaler) StartMonitoring(topic string, fn FunctionId) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	funcTotals, ok := a.totals[topic]
	if !ok {
		funcTotals = make(map[FunctionId]*metricsTotals)
		a.totals[topic] = funcTotals
	}

	_, ok = funcTotals[fn]
	if ok {
		return fmt.Errorf("Already monitoring topic %s and function %s", topic, fn)
	}

	funcTotals[fn] = &metricsTotals{}

	//a.scalers[fn] = decorate(a.metricsScaler(fn), a.queueLengthScaling(fn), a.limitScalingUp(fn), a.smooth(fn), a.limitScalingDown(fn), a.delay(fn))
	a.scalers[fn] = decorate(a.metricsScaler(fn), a.queueLengthScaling(fn), a.smooth(fn), a.exponentialSmoother(fn), a.step(fn), a.limitScalingUp(fn), a.limitScalingDown(fn), a.delay(fn))

	return nil
}

func (a *autoScaler) delay(fn FunctionId) adjuster {
	p := NewDelayer(func() time.Duration {
		return a.delayScaleDown(fn);
	})

	return func(proposal int) int {
		// FIXME: need to reset the delay if messages have been written to the input topic
		return p.Delay(proposal).Get()
	}
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
	delete(a.scalers, function)

	return nil
}

func (a *autoScaler) receiveLoop() {
	producerMetrics := a.metricsReceiver.ProducerMetrics()
	consumerMetrics := a.metricsReceiver.ConsumerMetrics()
	for {
		select {
		case pm, ok := <-producerMetrics:
			if ok { // ok should always be true
				a.receiveProducerMetric(pm)
			}

		case cm, ok := <-consumerMetrics:
			if ok { // ok should always be true
				a.receiveConsumerMetric(cm)
			}

		case <-a.stop:
			if receiver, ok := a.metricsReceiver.(io.Closer); ok {
				err := receiver.Close()
				if err != nil {
					log.Printf("Error closing metrics receiver: %v", err)
				}
			}
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
		mt, ok := funcTotals[FunctionId{cm.ConsumerGroup}]
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

type scaler func(*metricsTotals) int
type adjuster func(int) int

func decorate(s scaler, adjusters ...adjuster) scaler {
	for _, a := range adjusters {
		s = compose(a, s)
	}

	return s
}

func compose(a adjuster, s scaler) scaler {
	return func(mt *metricsTotals) int {
		return a(s(mt))
	}
}

func (a *autoScaler) metricsScaler(fn FunctionId) scaler {
	return func(mt *metricsTotals) int {
		//var proposedReplicas int
		//if mt.receiveCount == 0 {
		//	if mt.transmitCount == 0 {
		//		proposedReplicas = 0
		//	} else {
		//		proposedReplicas = 1 // arbitrary value
		//	}
		//} else {
		//	proposedReplicas = int(math.Ceil(float64(a.replicas[fn]) * float64(mt.transmitCount) / float64(mt.receiveCount)))
		//}
		//return proposedReplicas

		// Temporarily use the metricsScaler to prevent scaling to 0 if the input topic has been written to.
		if mt.transmitCount == 0 {
			return 0
		} else {
			return 1
		}
	}
}

func (a *autoScaler) queueLengthScaling(fn FunctionId) adjuster {
	//inner := newPidController(innerKp, innerKi, innerKd)
	outer := newPidController(outerKp, outerKi, outerKd, integratorPreload)
	//innerSetpoint := innerInitialSetpoint
	//queueLen := queueLength(0)

	return func(inputReplicas int) int {
		queueLen := a.queueLength(fn)

		// Use PID controller
		//innerSetpoint = queueRateOfChange(outer.work(int64(queueLen - outerSetpoint)))
		//
		//rateOfChange := queueRateOfChange(mt.transmitCount - mt.receiveCount)
		//deltaProposedReplicas := int(inner.work(int64(rateOfChange - innerSetpoint))) // let's hope this int64->int conversion doesn't truncate

		// Experiment with single PID controller based on queue length

		proposedReplicas := int(outer.work(int64(queueLen - outerSetpoint)))

		if proposedReplicas < 0 {
			proposedReplicas = 0
		}

		// Must not return 0 if the input is 1. See comment in metricsScaler.
		if proposedReplicas == 0 {
			return inputReplicas
		}

		return proposedReplicas

	}
}

func (a *autoScaler) limitScalingUp(fn FunctionId) adjuster {
	return func(proposedReplicas int) int {
		maxReplicas := a.maxReplicas(fn)
		possibleChange := proposedReplicas != a.replicas[fn]
		if proposedReplicas > maxReplicas {
			if possibleChange {
				log.Printf("Proposing %v should have maxReplicas (%d) instead of %d replicas", fn, maxReplicas, proposedReplicas)
			}
			proposedReplicas = maxReplicas
		}
		return proposedReplicas
	}
}

// smooth adds interpolation to replica count calculation, so that there are no sudden jumps.
func (a *autoScaler) smooth(id FunctionId) adjuster {
	memory := float32(0)
	return func(proposedReplicas int) int {
		// Improve responsiveness to workload increases by not smoothing out increases.
		if !smoothIncreases && float32(proposedReplicas) > memory {
			memory = float32(proposedReplicas)
			return proposedReplicas
		}

		newMemory := interpolate(memory, proposedReplicas, linearSmootherGreed)
		replicas := int(0.5 + newMemory)
		// Special case for the 0->1 case to have immediate scale up
		if proposedReplicas > 0 && a.replicas[id] == 0 {
			replicas = 1
			newMemory = 1.0
		}
		memory = newMemory
		return replicas
	}
}

func interpolate(current float32, target int, greed float32) float32 {
	return current + (float32(target)-current)*greed
}

// TODO: experiment with quartic (?) Savitzky-Golay filter
// https://uk.mathworks.com/help/curvefit/smoothing-data.html?s_tid=gn_loc_drop
func (a *autoScaler) exponentialSmoother(id FunctionId) adjuster {
	y := float32(0)
	return func(proposedReplicas int) int {
		// Improve responsiveness to workload increases by not smoothing out increases.
		if !smoothIncreases && float32(proposedReplicas) > y {
			y = float32(proposedReplicas)
			return proposedReplicas
		}

		x := float32(proposedReplicas)
		y = exponentialSmootherGreed*x + (1-exponentialSmootherGreed)*y
		return int(y)
	}
}

func (a *autoScaler) step(id FunctionId) adjuster {
	return func(proposedReplicas int) int {
		if proposedReplicas == 0 {
			return 0
		}
		return (1 + proposedReplicas/stepSize) * stepSize
	}
}

func (a *autoScaler) limitScalingDown(fn FunctionId) adjuster {
	return func(proposedReplicas int) int {
		// If zero replicas are proposed *and* there is already at least one replica, check the queue of work to the function.
		// The queue length is not allowed to initiate scaling up from 0 to 1 as that would confuse rate-based autoscaling.
		if proposedReplicas == 0 && a.replicas[fn] != 0 {
			empty, length := a.emptyQueue(fn)
			if !empty {
				// There may be work to do, so propose 1 replica instead.
				log.Printf("Ignoring proposal to scale %v to 0 replicas since queue length is %d", fn.Function, length)
				proposedReplicas = 1
			}
		}
		return proposedReplicas
	}
}

func (a *autoScaler) InformFunctionReplicas(function FunctionId, replicas int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.replicas[function] = replicas
}

func (a *autoScaler) Close() error {
	a.mutex.Lock()
	close(a.stop)
	<-a.accumulatingStopped
	a.mutex = nil // ensure autoScaler can no longer be used
	return nil
}
