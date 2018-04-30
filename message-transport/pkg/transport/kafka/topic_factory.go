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

package kafka

import (
	kazoo "github.com/wvanbergen/kazoo-go"
	"log"
	"github.com/projectriff/riff/message-transport/pkg/transport"
)

type KafkaProvisioner struct {
	kz *kazoo.Kazoo
}

func NewTopicFactory(connectionString string) transport.TopicFactory {
	kz, err := kazoo.NewKazooFromConnectionString(connectionString, nil)
	if err != nil {
		panic(err)
	}
	return &KafkaProvisioner{kz: kz}
}

func (kp *KafkaProvisioner) CreateTopic(topic string, maxConsumers int) error {
	err := kp.kz.CreateTopic(topic, maxConsumers, 1, nil)
	if err == kazoo.ErrTopicExists {
		log.Printf("Topic %s already exists. Doing nothing", topic)
		return nil
	}
	return err
}

