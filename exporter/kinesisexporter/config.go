// Copyright 2019 OpenTelemetry Authors
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

package kinesisexporter

import (
	"go.opentelemetry.io/collector/config/configmodels"
)

// AWSConfig contains AWS specific configuration such as kinesis stream, region, etc.
type AWSConfig struct {
	StreamName      string `mapstructure:"stream_name"`
	KinesisEndpoint string `mapstructure:"kinesis_endpoint"`
	Region          string `mapstructure:"region"`
	Role            string `mapstructure:"role"`
}

// KPLConfig contains kinesis producer library related config to controls things
// like aggregation, batching, connections, retries, etc.
type KPLConfig struct {
	AggregateBatchCount  int `mapstructure:"aggregate_batch_count"`
	AggregateBatchSize   int `mapstructure:"aggregate_batch_size"`
	BatchSize            int `mapstructure:"batch_size"`
	BatchCount           int `mapstructure:"batch_count"`
	BacklogCount         int `mapstructure:"backlog_count"`
	FlushIntervalSeconds int `mapstructure:"flush_interval_seconds"`
	MaxConnections       int `mapstructure:"max_connections"`
	MaxRetries           int `mapstructure:"max_retries"`
	MaxBackoffSeconds    int `mapstructure:"max_backoff_seconds"`
}

// Config contains the main configuration options for the kinesis exporter
type Config struct {
	configmodels.ExporterSettings `mapstructure:",squash"`

	AWS AWSConfig `mapstructure:"aws"`
	KPL KPLConfig `mapstructure:"kpl"`
}
