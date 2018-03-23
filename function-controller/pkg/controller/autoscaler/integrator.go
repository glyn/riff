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

import "time"

const windowSize = 100

type integrator struct {
	values    [windowSize]time.Duration
	numValues int
	nextIndex int
}

func newIntegrator() *integrator {
	return &integrator{
		numValues: 0,
		nextIndex: 0,
	}
}

func (i *integrator) add(value time.Duration) {
	i.values[i.nextIndex] = value
	i.nextIndex++
	if i.nextIndex >= windowSize {
		i.nextIndex = 0
	}
	if i.numValues < windowSize {
		i.numValues++
	}
}

func (i *integrator) get() time.Duration {
	if i.numValues == 0 {
		return 0
	}

	total := time.Duration(0)
	for j:= 0; j < i.numValues; j++ {
		total += i.values[j]
	}

	return total/time.Duration(i.numValues)
}
