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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/projectriff/riff/message-transport/pkg/transport/metrics"
	"github.com/projectriff/riff/message-transport/pkg/transport/metrics/mockmetrics"
	"time"
)

var _ = Describe("Autoscaler", func() {

	type AutoScalerInterfaces interface {
		AutoScaler

		// internal interfaces necessary to avoid test races
		receiveProducerMetric(pm metrics.ProducerAggregateMetric)
		receiveConsumerMetric(cm metrics.ConsumerAggregateMetric)
		takeSample()
	}

	const (
		testTopic = "test-topic"
	)

	var (
		auto                 AutoScalerInterfaces
		testFuncId           FunctionId
		testSamplingInterval time.Duration

		mockMetricsReceiver *mockmetrics.MetricsReceiver

		proposal map[FunctionId]int

		takeSample       func()
		shouldPropose    func(id FunctionId, expectedProposedReplicas int)
		shouldNotPropose func(id FunctionId)
	)

	BeforeEach(func() {
		testSamplingInterval = time.Millisecond * 10

		mockMetricsReceiver = &mockmetrics.MetricsReceiver{}
		auto = NewAutoScaler(mockMetricsReceiver, testSamplingInterval)

		testFuncId = FunctionId{"test-function"}

		takeSample = func() {
			auto.takeSample()
		}

		shouldPropose = func(funcId FunctionId, expectedProposedReplicas int) {
			replicas, ok := proposal[funcId]
			Expect(ok).To(BeTrue())
			Expect(replicas).To(Equal(expectedProposedReplicas)) // FIXME: use gomega to put the failure in the caller
		}

		shouldNotPropose = func(funcId FunctionId) {
			_, ok := proposal[funcId]
			Expect(ok).To(BeFalse())
		}
	})

	JustBeforeEach(func() {
		proposal = auto.Propose()
	})

	Describe("autoscaling", func() {
		BeforeEach(func() {
			Expect(auto.StartMonitoring(testTopic, testFuncId)).To(Succeed())
		})

		Context("when messages are produced but not consumed", func() {
			BeforeEach(func() {
				auto.receiveProducerMetric(metrics.ProducerAggregateMetric{
					Topic: testTopic,
					Count: 1,
				})

				takeSample()
			})

			It("should scale up to one", func() {
				shouldPropose(testFuncId, 1)
			})

			Context("when no further messages are produced", func() {
				BeforeEach(func() {
					takeSample()
				})

				It("should scale down to 0", func() {
					shouldPropose(testFuncId, 0)
				})
			})
		})

		Context("when messages are produced 3 times faster than they are consumed by 1 pod", func() {
			BeforeEach(func() {
				auto.InformFunctionReplicas(testFuncId, 1)

				auto.receiveProducerMetric(metrics.ProducerAggregateMetric{
					Topic: testTopic,
					Count: 3,
				})

				auto.receiveConsumerMetric(metrics.ConsumerAggregateMetric{
					Topic:    testTopic,
					Function: testFuncId.Function,
					Count:    1,
				})

				takeSample()
			})

			It("should scale up to 3 pods", func() {
				shouldPropose(testFuncId, 3)
			})

			Context("when no further messages are produced", func() {
				BeforeEach(func() {
					takeSample()
				})

				It("should scale down to 0", func() {
					shouldPropose(testFuncId, 0)
				})
			})

			Context("when messages are then produced 10 times faster than they are consumed by 3 pods", func() {
				BeforeEach(func() {
					auto.InformFunctionReplicas(testFuncId, 3)

					auto.receiveProducerMetric(metrics.ProducerAggregateMetric{
						Topic: testTopic,
						Count: 100,
					})

					auto.receiveConsumerMetric(metrics.ConsumerAggregateMetric{
						Topic:    testTopic,
						Function: testFuncId.Function,
						Count:    10,
					})

					takeSample()
				})

				It("should scale up to 30 pods", func() {
					shouldPropose(testFuncId, 30)
				})

				Context("when messages are then produced 10 times slower than they are consumed by 30 pods", func() {
					BeforeEach(func() {
						auto.InformFunctionReplicas(testFuncId, 30)

						auto.receiveProducerMetric(metrics.ProducerAggregateMetric{
							Topic: testTopic,
							Count: 10,
						})

						auto.receiveConsumerMetric(metrics.ConsumerAggregateMetric{
							Topic:    testTopic,
							Function: testFuncId.Function,
							Count:    100,
						})

						takeSample()
					})

					It("should scale down to 3 pods", func() {
						shouldPropose(testFuncId, 3)
					})
				})
			})
		})
	})

	Describe("monitoring lifecycle", func() {
		It("should return an empty proposal by default", func() {
			Expect(proposal).To(BeEmpty())
		})

		Context("when monitoring a given function is started", func() {
			BeforeEach(func() {
				Expect(auto.StartMonitoring(testTopic, testFuncId)).To(Succeed())
			})

			It("should propose zero replicas by default", func() {
				shouldPropose(testFuncId, 0)
			})

			It("should return a copy of the proposal map", func() {
				proposal = auto.Propose()
				replicas, ok := proposal[testFuncId]
				Expect(ok).To(BeTrue())
				Expect(replicas).To(Equal(0))

				proposal[testFuncId] = 99 // corrupt the map

				proposal = auto.Propose()
				replicas, ok = proposal[testFuncId]
				Expect(ok).To(BeTrue())
				Expect(replicas).To(Equal(0))

			})

			Context("when the function is running in one pod", func() {
				BeforeEach(func() {
					auto.InformFunctionReplicas(testFuncId, 1)
				})

				It("should propose zero replicas by default", func() {
					shouldPropose(testFuncId, 0)
				})
			})

			It("should return an error when monitoring the function is started again", func() {
				Expect(auto.StartMonitoring(testTopic, testFuncId)).To(MatchError("Already monitoring topic test-topic and function {test-function}"))
			})

			Context("when monitoring of the function is stopped", func() {
				BeforeEach(func() {
					Expect(auto.StopMonitoring(testTopic, testFuncId)).To(Succeed())
				})

				It("should not propose replicas for the function by default", func() {
					shouldNotPropose(testFuncId)
				})
			})
		})

		Context("when not monitoring a given function", func() {
			It("should not propose replicas for the function by default", func() {
				shouldNotPropose(testFuncId)
			})

			It("should return an error when monitoring the function is stopped", func() {
				Expect(auto.StopMonitoring(testTopic, testFuncId)).To(MatchError("Not monitoring topic test-topic and function {test-function}"))
			})

			Context("when the function is running in one pod", func() {
				BeforeEach(func() {
					auto.InformFunctionReplicas(testFuncId, 1)
				})

				It("should not propose any replicas for the function", func() {
					shouldNotPropose(testFuncId)
				})
			})
		})

		Context("when already monitoring a given function", func() {
			var anotherFuncId FunctionId

			BeforeEach(func() {
				anotherFuncId = FunctionId{"another-function"}
				Expect(auto.StartMonitoring(testTopic, testFuncId)).To(Succeed())
			})

			Context("when monitoring the same topic and another function is started", func() {
				BeforeEach(func() {
					Expect(auto.StartMonitoring(testTopic, anotherFuncId)).To(Succeed())
				})

				It("should propose zero replicas by default", func() {
					shouldPropose(anotherFuncId, 0)
				})
			})

			Context("when not monitoring another function", func() {
				It("should not propose replicas for the function by default", func() {
					shouldNotPropose(anotherFuncId)
				})
			})

			It("should return an error when monitoring another function is stopped", func() {
				Expect(auto.StopMonitoring(testTopic, anotherFuncId)).To(MatchError("Not monitoring topic test-topic and function {another-function}"))
			})
		})
	})

	Describe("constructor function", func() {
		It("should allow the sampling interval to default", func() {
			NewAutoScaler(mockMetricsReceiver)
		})

		It("should panic if more than one sampling interval is specified", func() {
			Expect(func() { NewAutoScaler(mockMetricsReceiver, time.Second, time.Second) }).To(Panic())
		})
	})

	// Exercise the autoscaler's goroutines minimally.
	Describe("Run", func() {
		var (
			producerMetrics chan metrics.ProducerAggregateMetric
			consumerMetrics chan metrics.ConsumerAggregateMetric
		)

		BeforeEach(func() {
			producerMetrics = make(chan metrics.ProducerAggregateMetric, 1)
			consumerMetrics = make(chan metrics.ConsumerAggregateMetric, 1)
			var pm <-chan metrics.ProducerAggregateMetric = producerMetrics
			mockMetricsReceiver.On("ProducerMetrics").Return(pm)
			var cm <-chan metrics.ConsumerAggregateMetric = consumerMetrics
			mockMetricsReceiver.On("ConsumerMetrics").Return(cm)

			auto = NewAutoScaler(mockMetricsReceiver, time.Millisecond*10)
			auto.Run()

			Expect(auto.StartMonitoring(testTopic, testFuncId)).To(Succeed())
		})

		AfterEach(func() {
			Expect(auto.Close()).To(Succeed())
		})

		Context("when metrics are produced and consumed and then not produced for a while", func() {
			BeforeEach(func() {
				producerMetrics <- metrics.ProducerAggregateMetric{
					Topic: testTopic,
				}

				consumerMetrics <- metrics.ConsumerAggregateMetric{
					Topic: testTopic,
				}

				time.Sleep(testSamplingInterval * 3)
			})

			It("should scale down to 0", func() {
				shouldPropose(testFuncId, 0)
			})
		})
	})
})
