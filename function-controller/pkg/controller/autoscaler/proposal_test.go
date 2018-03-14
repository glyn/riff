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

package autoscaler_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/projectriff/riff/function-controller/pkg/controller/autoscaler"
)

var _ = Describe("Proposal", func() {

	const requiredScaleDownProposals = 10

	var (
		proposal *autoscaler.Proposal
	)

	BeforeEach(func() {
		proposal = autoscaler.NewProposal(requiredScaleDownProposals)
	})

	It("should initially propose zero", func() {
		Expect(proposal.Get()).To(Equal(0))
	})

	Context("when scaled up", func() {
		BeforeEach(func() {
			proposal.Propose(1)
		})

		It("should immediately propose the scaled up value", func() {
			Expect(proposal.Get()).To(Equal(1))

		})

		Context("when scaled down insufficiently often", func() {
			BeforeEach(func() {
				for i := 0; i < requiredScaleDownProposals-1; i++ {
					proposal.Propose(0)
				}
			})

			It("should continue to propose the scaled up value", func() {
				Expect(proposal.Get()).To(Equal(1))

			})
		})

		Context("when scaled back down sufficiently often", func() {
			BeforeEach(func() {
				for i := 0; i < requiredScaleDownProposals; i++ {
					proposal.Propose(0)
				}
			})

			It("should propose the scaled down value", func() {
				Expect(proposal.Get()).To(Equal(0))

			})
		})
	})

	Context("when scaled up", func() {
		BeforeEach(func() {
			proposal.Propose(10)
		})

		It("should immediately propose the scaled up value", func() {
			Expect(proposal.Get()).To(Equal(10))

		})

		Context("when scaled down in stages but insufficiently often", func() {
			BeforeEach(func() {
				for i := 0; i < 5; i++ {
					proposal.Propose(8)
				}
				for i := 0; i < requiredScaleDownProposals-6; i++ {
					proposal.Propose(4)
				}
			})

			It("should continue to propose the scaled up value", func() {
				Expect(proposal.Get()).To(Equal(10))
			})
		})

		Context("when scaled down in stages but insufficiently often and with a 'blip'", func() {
			BeforeEach(func() {
				for i := 0; i < 5; i++ {
					proposal.Propose(8)
				}
				proposal.Propose(9)
				for i := 0; i < requiredScaleDownProposals-7; i++ {
					proposal.Propose(4)
				}
			})

			It("should continue to propose the scaled up value", func() {
				Expect(proposal.Get()).To(Equal(10))
			})
		})

		Context("when scaled back down in stages sufficiently often", func() {
			BeforeEach(func() {
				for i := 0; i < 5; i++ {
					proposal.Propose(8)
				}
				for i := 0; i < requiredScaleDownProposals-5; i++ {
					proposal.Propose(4)
				}
			})

			It("should propose the final scaled down value", func() {
				Expect(proposal.Get()).To(Equal(4))
			})

			Context("when scaled down again but insufficiently often", func() {
				BeforeEach(func() {
					proposal.Propose(2)
				})

				It("should propose the previous scaled down value", func() {
					Expect(proposal.Get()).To(Equal(4))
				})
			})
		})
	})
})
