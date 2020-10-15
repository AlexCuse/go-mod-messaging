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
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"strconv"
	"strings"
	"sync"
	"time"

	kc "github.com/segmentio/kafka-go"
)

// MessageMarshaler defines the function signature for marshaling structs into []byte.  Prior to final marshaling payload should be placed in a MessageEnvelope
type MessageMarshaler func(v interface{}) ([]byte, error)

func DefaultMessageMarshaler(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// MessageUnmarshaler defines the function signature for unmarshaling []byte into a message envelope for bus.
type MessageUnmarshaler func(data []byte, target *types.MessageEnvelope) error

func DefaultMessageUnmarshaler(data []byte, target *types.MessageEnvelope) error {
	return json.Unmarshal(data, target)
}

// Client facilitates communication to an Kafka server and provides functionality needed to send and receive Kafka
// messages.
type kafkaClient struct {
	options     types.MessageBusConfig
	marshaler   MessageMarshaler
	unmarshaler MessageUnmarshaler
	dialer      *kc.Dialer
	done        chan struct{}
	writers     sync.Map
	readers     sync.Map
}

type readerChannel struct {
	reader  *kc.Reader
	channel <-chan kc.Message
}

func (c *kafkaClient) readerFactory(topic string, errors chan error) readerChannel {
	cached, exists := c.readers.Load(topic)

	if exists {
		return cached.(readerChannel)
	}

	readerConfig := kc.ReaderConfig{
		Brokers: []string{c.brokerAddress()},
		Topic:   topic,
		Dialer:  c.dialer,
	}

	if group := c.options.Optional["ConsumerGroupID"]; len(group) > 0 {
		readerConfig.GroupID = group
		if part := c.options.Optional["PartitionID"]; len(part) > 0 {
			if partition, err := strconv.Atoi(part); err != nil {
				panic(err)
			} else {
				readerConfig.Partition = partition
			}
		}
	}

	if ci := c.options.Optional["CommitInterval"]; len(ci) > 0 {
		if commitInterval, err := time.ParseDuration(ci); err != nil {
			panic(err)
		} else {
			readerConfig.CommitInterval = commitInterval
		}
	}

	reader := kc.NewReader(readerConfig)

	if val := c.options.Optional["StartOffset"]; len(val) > 0 {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			panic(err)
		} else {
			reader.SetOffset(i)
		}
	} else if readerConfig.GroupID != "" {
		reader.SetOffset(kc.LastOffset)
	}

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

	result := readerChannel{
		reader:  reader,
		channel: readerChan,
	}

	c.readers.Store(topic, result)
	return result
}

func (c *kafkaClient) brokerAddress() string {
	return c.options.PublishHost.Host + ":" + strconv.Itoa(c.options.PublishHost.Port)
}

func (c *kafkaClient) writerFactory(topic string) *kc.Writer {
	cached, exists := c.writers.Load(topic)

	if exists {
		return cached.(*kc.Writer)
	}

	writer := &kc.Writer{
		Addr:     kc.TCP(c.brokerAddress()),
		Topic:    topic,
		Balancer: &kc.LeastBytes{},
	}

	if req, found := c.options.Optional["RequiredAcks"]; found {
		switch strings.ToLower(req) {
		case "all", "-1":
			writer.RequiredAcks = kc.RequireAll
			break
		case "one", "1":
			writer.RequiredAcks = kc.RequireOne
			break
		case "none", "0":
			writer.RequiredAcks = kc.RequireNone
			break
		}
	}

	c.writers.Store(topic, writer)
	return writer
}

// NewKafkaClient constructs a new Kafka kafkaClient based on the options provided.
func NewKafkaClient(options types.MessageBusConfig) (*kafkaClient, error) {
	kc := kafkaClient{
		options:     options,
		marshaler:   DefaultMessageMarshaler,
		unmarshaler: DefaultMessageUnmarshaler,
		dialer: &kc.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		done:    make(chan struct{}),
		readers: sync.Map{},
		writers: sync.Map{},
	}

	if clientId := options.Optional["ClientID"]; len(clientId) > 0 {
		kc.dialer.ClientID = clientId
	}
	return &kc, nil
}

// Options for customizing client behavior
type KafkaClientOptions struct {
	Marshaler   MessageMarshaler
	Unmarshaler MessageUnmarshaler
}

// NewKafkaClient constructs a new Kafka kafkaClient based on the options provided.
func NewKafkaClientWithAdapter(messageBusConfig types.MessageBusConfig, options KafkaClientOptions) (*kafkaClient, error) {
	ct, err := NewKafkaClient(messageBusConfig)

	if err != nil {
		return nil, err
	}

	if options.Marshaler != nil {
		ct.marshaler = options.Marshaler
	}

	if options.Unmarshaler != nil {
		ct.unmarshaler = options.Unmarshaler
	}

	return ct, err
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
		r := mc.readerFactory(topic.Topic, messageErrors)

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
		}(r.reader, r.channel, topic.Messages)
	}

	return nil
}

type closer interface {
	Close() error
}

func disconnect(k interface{}, val interface{}) bool {
	ready, ok := val.(closer)

	if ok {
		err := ready.Close()
		//TODO: handle
		if err != nil {
			fmt.Println(fmt.Sprintf("Error closing reader: %+v", err))
		}
	}

	return ok
}

// Disconnect closes the connection to the connected Kafka server.
func (mc *kafkaClient) Disconnect() error {
	close(mc.done)

	mc.writers.Range(disconnect)
	mc.readers.Range(disconnect)

	return nil
}
