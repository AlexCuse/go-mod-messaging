//
// Copyright (c) 2019 Intel Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package messaging

import (
	"fmt"
	"strings"

	"github.com/edgexfoundry/go-mod-messaging/v2/internal/pkg/mqtt"
	"github.com/edgexfoundry/go-mod-messaging/v2/internal/pkg/redis"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
)

const (
	// MQTT messaging implementation
	MQTT = "mqtt"

	// Redis Pub/Sub messaging implementation
	Redis = "redis"
)

var customClientFactories = make(map[string]func(config types.MessageBusConfig) (MessageClient, error))

// RegisterCustomClientFactory allows registering a custom client with the factory for use elsewhere
// eg app sdk MessageBus trigger
func RegisterCustomClientFactory(name string, builder func(config types.MessageBusConfig) (MessageClient, error)) error {
	if name == "" {
		return fmt.Errorf("name is required to register a custom factory")
	}

	if name == MQTT || name == Redis {
		return fmt.Errorf("cannot register custom factory for built in type '%s'", name)
	}

	customClientFactories[name] = builder

	return nil
}

// NewMessageClient is a factory function to instantiate different message client depending on
// the "Type" from the configuration
func NewMessageClient(msgConfig types.MessageBusConfig) (MessageClient, error) {

	if msgConfig.PublishHost.IsHostInfoEmpty() && msgConfig.SubscribeHost.IsHostInfoEmpty() {
		return nil, fmt.Errorf("unable to create messageClient: host info not set")
	}

	switch lowerMsgType := strings.ToLower(msgConfig.Type); lowerMsgType {
	case MQTT:
		return mqtt.NewMQTTClient(msgConfig)
	case Redis:
		return redis.NewClient(msgConfig)
	default:
		if builder, found := customClientFactories[lowerMsgType]; found {
			return builder(msgConfig)
		}
		return nil, fmt.Errorf("unknown message type '%s' requested", msgConfig.Type)
	}
}
