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
	"fmt"
	"sync"
	"time"
	"log"
	"github.com/projectriff/riff/message-transport/pkg/transport"
)

//go:generate mockery -name=AutoScaler -output mockautoscaler -outpkg mockautoscaler

type AutoScaler interface {
	// Set maximum replica count policy.
	SetMaxReplicasPolicy(func(function FunctionId) int)

	// Set delay scale down policy.
	SetDelayScaleDownPolicy(func(function FunctionId) time.Duration)

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

// Go does not provide a maximum int, so we have to calculate it.
const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)

// NewAutoScaler constructs an autoscaler instance using the given transport inspector.
func NewAutoScaler(transportInspector transport.Inspector) *autoScaler {
	return &autoScaler{
		mutex:              &sync.Mutex{},
		transportInspector: transportInspector,
		functions:          make(map[FunctionId]string),
		scalers:            make(map[FunctionId]scaler),
		replicas:           make(map[FunctionId]int),
		maxReplicas:        func(function FunctionId) int { return maxInt },
		delayScaleDown:     func(function FunctionId) time.Duration { return time.Duration(0) },
	}
}

func (a *autoScaler) SetMaxReplicasPolicy(maxReplicas func(function FunctionId) int) {
	a.maxReplicas = maxReplicas
}

func (a *autoScaler) SetDelayScaleDownPolicy(delayScaleDown func(function FunctionId) time.Duration) {
	a.delayScaleDown = delayScaleDown
}

type autoScaler struct {
	mutex               *sync.Mutex // nil when autoScaler is closed
	transportInspector  transport.Inspector
	functions           map[FunctionId]string
	scalers             map[FunctionId]scaler
	replicas            map[FunctionId]int // tracks all functions, including those which are not being monitored
	maxReplicas         func(function FunctionId) int
	delayScaleDown      func(function FunctionId) time.Duration
	stop                chan struct{}
	accumulatingStopped chan struct{}
}

func (a *autoScaler) Propose() map[FunctionId]int {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	proposals := make(map[FunctionId]int)
	for fn, topic := range a.functions {
		len, err := a.transportInspector.QueueLength(topic, fn.Function)
		if err != nil {
			log.Printf("Failed to obtain queue length (and will assume it is positive): %v", err)
			len = 1
		}
		proposals[fn] = a.scalers[fn](len)
		// proposals[fn] = max(proposals[fn], a.scalers[fn](mt)) might help multiple input topics
	}

	return proposals
}

func (a *autoScaler) emptyQueue(funcId FunctionId) (bool, int64) {
	if topic, ok := a.functions[funcId]; ok {
		queueLen, err := a.transportInspector.QueueLength(topic, funcId.Function)
		if err != nil {
			log.Printf("Failed to obtain queue length (and will assume it is positive): %v", err)
			return false, -1
		}
		if queueLen > 0 {
			return false, queueLen
		}
	}
	return true, 0
}

func (a *autoScaler) StartMonitoring(topic string, fn FunctionId) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if t, ok := a.functions[fn]; ok && t != topic {
		panic("Functions with multiple input topics are not supported")
	}

	a.functions[fn] = topic

	a.scalers[fn] = decorate(a.queueLengthScaler(fn), a.limitScalingUp(fn), /*a.limitScalingDown(fn),*/ a.delay(fn))

	return nil
}

func (a *autoScaler) delay(fn FunctionId) adjuster {
	p := NewDelayer(func() time.Duration {
		return a.delayScaleDown(fn);
	})

	return func(proposal int) int {
		return p.Delay(proposal).Get()
	}
}

func (a *autoScaler) StopMonitoring(topic string, function FunctionId) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if t, ok := a.functions[function]; !ok || t != topic {
		return fmt.Errorf("Not monitoring topic %s and function %s", topic, function)
	}

	delete(a.functions, function)
	delete(a.scalers, function)

	return nil
}

type scaler func(int64) int
type adjuster func(int) int

func decorate(s scaler, adjusters ...adjuster) scaler {
	for _, a := range adjusters {
		s = compose(a, s)
	}

	return s
}

func compose(a adjuster, s scaler) scaler {
	return func(qLen int64) int {
		return a(s(qLen))
	}
}

func (a *autoScaler) queueLengthScaler(fn FunctionId) scaler {
	return func(qLen int64) int {
		var proposedReplicas int = 0
		return proposedReplicas

		/* current algorithm
		 1. computeDesiredReplicas returns replicaCounts and activityCounts:
		   * replicaCounts is a mapping from function to wanted number of replicas
		   * activityCounts is a mapping from function to combined activity marker (we're using sum of position across all partitions and all topics)

		2. smooth smooths out the replica counts and ignores the activityCounts

		3. delay delays scaling:
		   * to 0 until idleTimeout has passed or, if the queue length is non-zero the scale to 0 clock is reset
		   * UP until there is some change in the combined activity marker in case rebalancing is happening


		 */

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

//func (a *autoScaler) limitScalingDown(fn FunctionId) adjuster {
//	return func(proposedReplicas int) int {
//		// If zero replicas are proposed *and* there is already at least one replica, check the queue of work to the function.
//		// The queue length is not allowed to initiate scaling up from 0 to 1 as that would confuse rate-based autoscaling.
//		if proposedReplicas == 0 && a.replicas[fn] != 0 {
//			empty, length := a.emptyQueue(fn)
//			if !empty {
//				// There may be work to do, so propose 1 replica instead.
//				log.Printf("Ignoring proposal to scale %v to 0 replicas since queue length is %d", fn.Function, length)
//				proposedReplicas = 1
//			}
//		}
//		return proposedReplicas
//	}
//}

func (a *autoScaler) InformFunctionReplicas(function FunctionId, replicas int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.replicas[function] = replicas
}
