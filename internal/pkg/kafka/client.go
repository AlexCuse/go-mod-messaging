/********************************************************************************
 *  Copyright 2020 Technotects
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/

package kafka

import (
	"context"
	"encoding/json"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"strconv"
	"time"

	kc "github.com/segmentio/kafka-go"
)

// MessageMarshaler defines the function signature for marshaling structs into []byte.
type MessageMarshaler func(v interface{}) ([]byte, error)

// MessageUnmarshaler defines the function signature for unmarshaling []byte into structs.
type MessageUnmarshaler func(data []byte, v interface{}) error

// Client facilitates communication to an Kafka server and provides functionality needed to send and receive Kafka
// messages.
type kafkaClient struct {
	options     types.MessageBusConfig
	marshaler   MessageMarshaler
	unmarshaler MessageUnmarshaler
	dialer      *kc.Dialer
	done        chan struct{}
	writers     map[string]*kc.Writer
	readers     map[string]*kc.Reader
}

//TODO: mutex around map access
func (c *kafkaClient) readerFactory(topic string) *kc.Reader {
	reader, exists := c.readers[topic]

	if exists {
		return reader
	}

	reader = kc.NewReader(kc.ReaderConfig{
		Brokers: []string{c.brokerAddress()},
		Topic:   topic,
		Dialer:  c.dialer,
	})

	c.readers[topic] = reader
	return reader
}

func (c *kafkaClient) brokerAddress() string {
	return c.options.PublishHost.Host + ":" + strconv.Itoa(c.options.PublishHost.Port)
}

func (c *kafkaClient) readerChannelFactory(reader *kc.Reader, errors chan error) <-chan kc.Message {
	readerChan := make(chan kc.Message)

	//TODO: handle exits
	go func() {
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				errors <- err
			} else {
				readerChan <- msg
			}
		}
	}()

	return readerChan
}

//TODO: mutex around map access
func (c *kafkaClient) writerFactory(topic string) *kc.Writer {
	writer, exists := c.writers[topic]

	if exists {
		return writer
	}

	writer = &kc.Writer{
		Addr:     kc.TCP(c.brokerAddress()),
		Topic:    topic,
		Balancer: &kc.LeastBytes{},
	}

	c.writers[topic] = writer
	return writer
}

// NewKafkaClient constructs a new Kafka kafkaClient based on the options provided.
func NewKafkaClient(options types.MessageBusConfig) (*kafkaClient, error) {
	return &kafkaClient{
		options:     options,
		marshaler:   json.Marshal,
		unmarshaler: json.Unmarshal,
		dialer: &kc.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		done:    make(chan struct{}),
		readers: make(map[string]*kc.Reader),
		writers: make(map[string]*kc.Writer),
	}, nil
}

// Connect establishes a connection to a Kafka server.
// This must be called before any other functionality provided by the Client.
func (mc *kafkaClient) Connect() error {
	//no-op for now
	return nil
}

// Publish sends a message to the connected Kafka server.
func (mc *kafkaClient) Publish(message types.MessageEnvelope, topic string) error {
	//only supporting JSON at the moment
	marshaledMessage, err := mc.marshaler(message)
	if err != nil {
		return NewOperationErr(PublishOperation, err.Error())
	}

	writer := mc.writerFactory(topic)

	if err != nil {
		return NewOperationErr(PublishOperation, err.Error())
	}

	err = writer.WriteMessages(context.Background(), kc.Message{
		Key:   []byte(message.CorrelationID),
		Value: marshaledMessage,
	})

	if err != nil {
		if e, ok := err.(kc.WriteErrors); ok {
			err = e[0]
		}
		return NewOperationErr(PublishOperation, err.Error())
	}

	return nil
}

// Subscribe creates a subscription for the specified topics.
func (mc *kafkaClient) Subscribe(topics []types.TopicChannel, messageErrors chan error) error {
	for _, topic := range topics {
		r := mc.readerFactory(topic.Topic)
		rc := mc.readerChannelFactory(r, messageErrors)

		go func(r *kc.Reader, input <-chan kc.Message, output chan<- types.MessageEnvelope) {
			for {
				select {
				case msg := <-input:
					formattedMessage := types.MessageEnvelope{}

					err := mc.unmarshaler(msg.Value, &formattedMessage)

					if err != nil {
						messageErrors <- err
					} else {
						output <- formattedMessage
					}
					//TODO: retry config
					r.CommitMessages(context.Background(), msg)
				case <-mc.done:
					return
				}
			}
		}(r, rc, topic.Messages)
	}

	return nil
}

// Disconnect closes the connection to the connected Kafka server.
func (mc *kafkaClient) Disconnect() error {
	close(mc.done)

	//TODO: handle close errors
	for _, x := range mc.writers {
		x.Close()
	}
	for _, x := range mc.readers {
		x.Close()
	}

	return nil
}
