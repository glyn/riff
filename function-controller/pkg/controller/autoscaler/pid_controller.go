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

const dt = 1 // sampling rate

type pidController struct {
	kp   float64 // proportional coefficient
	ki   float64 // integral coefficient
	kd   float64 // derivative coefficient
	i    int64 // integral
	d    int64 // derivative
	prev int64
}

func newPidController(kp float64, ki float64, kd float64) *pidController {
	return &pidController{
		kp: kp,
		ki: ki,
		kd: kd,
	}
}

// e is the difference between the setpoint and the actual value
func (pid *pidController) work(e int64) int64 {
	pid.i += e * dt             // integrate error over time. should not overflow unless the feedback function is unstable
	pid.d = (e - pid.prev) / dt // differentiate error over time
	pid.prev = e

	// The control value is sum of the proportional, integral, and derivative terms adjusted by the
	// corresponding coefficients.
	return int64(pid.kp*float64(e) + pid.ki*float64(pid.i) + pid.kd*float64(pid.d))
}
