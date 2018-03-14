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

type Proposal struct {
	delayLimit    int
	value         int
	previousValue int
	delay         int
}

// NewProposal creates a proposal for which scaling down only takes effect after the given number of proposals.
func NewProposal(requiredScaleDownProposals int) *Proposal {
	if requiredScaleDownProposals < 1 {
		panic("requiredScaleDownProposals must be positive")
	}
	return &Proposal{
		delayLimit: requiredScaleDownProposals - 1,
	}
}

func (b *Proposal) Propose(proposal int) {
	if proposal < 0 {
		panic("Proposed numbers of replicas must be non-negative")
	}

	if proposal > b.value {
		if b.delay > 0 && proposal < b.previousValue {
			b.value = proposal
			b.delay--
		} else {
			// Scale up immediately
			b.value = proposal
			b.previousValue = proposal
			b.delay = 0
		}
	} else if proposal < b.value {
		// Defer scaling down
		if b.delay == 0 {
			b.delay = b.delayLimit
			b.previousValue = b.value
		} else {
			// Include this scaling down in the previous delay
			b.delay--
		}
		b.value = proposal
	} else if b.delay > 0 {
		// Scaling down is deferred and the proposal is unchanged, so bring forward scaling down
		b.delay--
	}
}

func (b *Proposal) Get() int {
	if b.delay > 0 {
		return b.previousValue
	}
	return b.value
}
