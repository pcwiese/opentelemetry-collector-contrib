// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datadogexporter

import (
	"context"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/exportable/config/configdefs"
	"github.com/DataDog/datadog-agent/pkg/trace/exportable/obfuscate"
	"github.com/DataDog/datadog-agent/pkg/trace/exportable/pb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"gopkg.in/zorkian/go-datadog-api.v2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/utils"
)

type traceExporter struct {
	logger         *zap.Logger
	cfg            *config.Config
	edgeConnection TraceEdgeConnection
	obfuscator     *obfuscate.Obfuscator
	client         *datadog.Client
}

var (
	obfuscatorConfig = &configdefs.ObfuscationConfig{
		ES: configdefs.JSONObfuscationConfig{
			Enabled: true,
		},
		Mongo: configdefs.JSONObfuscationConfig{
			Enabled: true,
		},
		HTTP: configdefs.HTTPObfuscationConfig{
			RemoveQueryString: true,
			RemovePathDigits:  true,
		},
		RemoveStackTraces: true,
		Redis:             configdefs.Enablable{Enabled: true},
		Memcached:         configdefs.Enablable{Enabled: true},
	}
)

func newTraceExporter(params component.ExporterCreateParams, cfg *config.Config) (*traceExporter, error) {
	// client to send running metric to the backend & perform API key validation
	client := utils.CreateClient(cfg.API.Key, cfg.Metrics.TCPAddr.Endpoint)
	utils.ValidateAPIKey(params.Logger, client)

	// removes potentially sensitive info and PII, approach taken from serverless approach
	// https://github.com/DataDog/datadog-serverless-functions/blob/11f170eac105d66be30f18eda09eca791bc0d31b/aws/logs_monitoring/trace_forwarder/cmd/trace/main.go#L43
	obfuscator := obfuscate.NewObfuscator(obfuscatorConfig)

	exporter := &traceExporter{
		logger:         params.Logger,
		cfg:            cfg,
		edgeConnection: CreateTraceEdgeConnection(cfg.Traces.TCPAddr.Endpoint, cfg.API.Key, params.ApplicationStartInfo),
		obfuscator:     obfuscator,
		client:         client,
	}

	return exporter, nil
}

// TODO: when component.Host exposes a way to retrieve processors, check for batch processors
// and log a warning if not set

// Start tells the exporter to start. The exporter may prepare for exporting
// by connecting to the endpoint. Host parameter can be used for communicating
// with the host after Start() has already returned. If error is returned by
// Start() then the collector startup will be aborted.
// func (exp *traceExporter) Start(_ context.Context, _ component.Host) error {
// 	return nil
// }

func (exp *traceExporter) pushTraceData(
	ctx context.Context,
	td pdata.Traces,
) (int, error) {

	// convert traces to datadog traces and group trace payloads by env
	// we largely apply the same logic as the serverless implementation, simplified a bit
	// https://github.com/DataDog/datadog-serverless-functions/blob/f5c3aedfec5ba223b11b76a4239fcbf35ec7d045/aws/logs_monitoring/trace_forwarder/cmd/trace/main.go#L61-L83
	ddTraces, err := ConvertToDatadogTd(td, exp.cfg)

	if err != nil {
		exp.logger.Info("failed to convert traces", zap.Error(err))
		return 0, err
	}

	// group the traces by env to reduce the number of flushes
	aggregatedTraces := AggregateTracePayloadsByEnv(ddTraces)

	// security/obfuscation for db, query strings, stack traces, pii, etc
	// TODO: is there any config we want here? OTEL has their own pipeline for regex obfuscation
	ObfuscatePayload(exp.obfuscator, aggregatedTraces)

	pushTime := time.Now().UTC().UnixNano()
	for _, ddTracePayload := range aggregatedTraces {
		// currently we don't want to do retries since api endpoints may not dedupe in certain situations
		// adding a helper function here to make custom retry logic easier in the future
		exp.pushWithRetry(ctx, ddTracePayload, 1, pushTime, func() error {
			return nil
		})
	}

	ms := metrics.RunningMetric("traces", uint64(pushTime), exp.logger, exp.cfg)
	exp.client.PostMetrics(ms)

	return len(aggregatedTraces), nil
}

// gives us flexibility to add custom retry logic later
func (exp *traceExporter) pushWithRetry(ctx context.Context, ddTracePayload *pb.TracePayload, maxRetries int, pushTime int64, fn func() error) error {
	err := exp.edgeConnection.SendTraces(ctx, ddTracePayload, maxRetries)

	if err != nil {
		exp.logger.Info("failed to send traces", zap.Error(err))
	}

	// this is for generating metrics like hits, errors, and latency, it uses a separate endpoint than Traces
	stats := ComputeAPMStats(ddTracePayload, pushTime)
	errStats := exp.edgeConnection.SendStats(context.Background(), stats, maxRetries)

	if errStats != nil {
		exp.logger.Info("failed to send trace stats", zap.Error(errStats))
	}

	return fn()
}
