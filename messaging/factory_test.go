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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"

	"github.com/stretchr/testify/assert"
)

var msgConfig = types.MessageBusConfig{
	PublishHost: types.HostInfo{
		Host:     "*",
		Port:     5570,
		Protocol: "tcp",
	},
}

func TestRegisterCustomClientFactory_MissingName(t *testing.T) {
	err := RegisterCustomClientFactory("", nil)

	require.Error(t, err)
	assert.Equal(t, "name is required to register a custom factory", err.Error())
}

func TestRegisterCustomClientFactory_BuiltInType(t *testing.T) {
	err := RegisterCustomClientFactory("mqtt", nil)

	require.Error(t, err)
	assert.Equal(t, "cannot register custom factory for built in type 'mqtt'", err.Error())
}

func TestNewMessageClientZeroMq(t *testing.T) {
	msgConfig.Type = ZeroMQ
	_, err := NewMessageClient(msgConfig)

	require.NoError(t, err, "New Message client failed: ", err)
}

func TestNewMessageClientCustom(t *testing.T) {
	msgConfig.Type = "custom"

	err := RegisterCustomClientFactory("custom", func(config types.MessageBusConfig) (MessageClient, error) {
		return nil, nil
	})

	require.NoError(t, err)

	_, err = NewMessageClient(msgConfig)

	require.NoError(t, err)
}

func TestNewMessageClientMQTT(t *testing.T) {
	messageBusConfig := msgConfig
	messageBusConfig.Type = MQTT
	messageBusConfig.Optional = map[string]string{
		"Username":          "TestUser",
		"Password":          "TestPassword",
		"ClientId":          "TestClientID",
		"Topic":             "TestTopic",
		"Qos":               "1",
		"KeepAlive":         "3",
		"Retained":          "true",
		"ConnectionPayload": "TestConnectionPayload",
	}

	_, err := NewMessageClient(messageBusConfig)

	require.NoError(t, err, "New Message client failed: ", err)
}

func TestNewMessageBogusType(t *testing.T) {
	msgConfig.Type = "bogus"

	_, err := NewMessageClient(msgConfig)
	require.Error(t, err, "Expected message type error")
}

func TestNewMessageEmptyHostAndPortNumber(t *testing.T) {
	msgConfig.PublishHost.Host = ""
	msgConfig.PublishHost.Port = 0
	_, err := NewMessageClient(msgConfig)
	require.Error(t, err, "Expected message type error")
}
