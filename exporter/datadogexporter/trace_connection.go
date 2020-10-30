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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/exportable/pb"
	"github.com/DataDog/datadog-agent/pkg/trace/exportable/stats"
	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/collector/component"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/utils"
)

// TraceEdgeConnection is used to send data to trace edge
type TraceEdgeConnection interface {
	SendTraces(ctx context.Context, trace *pb.TracePayload, maxRetries int) error
	SendStats(ctx context.Context, stats *stats.Payload, maxRetries int) error
}

type traceEdgeConnection struct {
	traceURL           string
	statsURL           string
	apiKey             string
	startInfo          component.ApplicationStartInfo
	InsecureSkipVerify bool
}

const (
	traceEdgeTimeout       time.Duration = 10 * time.Second
	traceEdgeRetryInterval time.Duration = 10 * time.Second
)

// CreateTraceEdgeConnection returns a new TraceEdgeConnection
func CreateTraceEdgeConnection(rootURL, apiKey string, startInfo component.ApplicationStartInfo) TraceEdgeConnection {

	return &traceEdgeConnection{
		traceURL:  rootURL + "/api/v0.2/traces",
		statsURL:  rootURL + "/api/v0.2/stats",
		startInfo: startInfo,
		apiKey:    apiKey,
	}
}

// Payload represents a data payload to be sent to some endpoint
type Payload struct {
	CreationDate time.Time
	Bytes        []byte
	Headers      map[string]string
}

// SendTraces serializes a trace payload to protobuf and sends it to Trace Edge
func (con *traceEdgeConnection) SendTraces(ctx context.Context, trace *pb.TracePayload, maxRetries int) error {
	binary, marshallErr := proto.Marshal(trace)
	if marshallErr != nil {
		return fmt.Errorf("failed to serialize trace payload to protobuf: %w", marshallErr)
	}
	if len(trace.Traces) == 0 {
		return fmt.Errorf("no traces in payload")
	}

	// Set Headers
	headers := utils.ProtobufHeaders

	// Construct a Payload{} from the headers and binary
	payload := Payload{
		CreationDate: time.Now().UTC(),
		Bytes:        binary,
		Headers:      headers,
	}

	var sendErr error
	var shouldRetry bool
	// If error while sending to trace-edge, retry maximum maxRetries number of times
	// NOTE: APM stores traces by trace id, however, Logs pipeline does NOT dedupe APM events,
	// and retries may potentially cause duplicate APM events in Trace Search
	for retries := 1; retries <= maxRetries; retries++ {
		if shouldRetry, sendErr = con.sendPayloadToTraceEdge(ctx, con.apiKey, &payload, con.traceURL); sendErr == nil {
			return nil
		}

		if !shouldRetry {
			break
		}

		time.Sleep(traceEdgeRetryInterval)
	}
	return fmt.Errorf("failed to send trace payload to trace edge: %w", sendErr)
}

// SendStats serializes a stats payload to json and sends it to Trace Edge
func (con *traceEdgeConnection) SendStats(ctx context.Context, sts *stats.Payload, maxRetries int) error {
	var b bytes.Buffer
	err := stats.EncodePayload(&b, sts)
	if err != nil {
		return fmt.Errorf("failed to encode stats payload: %w", err)
	}
	binary := b.Bytes()

	// Set Headers
	headers := utils.JSONHeaders

	// Construct a Payload{} from the headers and binary
	payload := Payload{
		CreationDate: time.Now().UTC(),
		Bytes:        binary,
		Headers:      headers,
	}

	var sendErr error
	var shouldRetry bool
	// If error while sending to trace-edge, retry maximum maxRetries number of times
	// NOTE: APM does NOT dedupe, and retries may potentially cause duplicate/inaccurate stats
	for retries := 1; retries <= maxRetries; retries++ {
		if shouldRetry, sendErr = con.sendPayloadToTraceEdge(ctx, con.apiKey, &payload, con.statsURL); sendErr == nil {
			return nil
		}

		if !shouldRetry {
			break
		}

		time.Sleep(traceEdgeRetryInterval)
	}
	return fmt.Errorf("failed to send stats payload to trace edge: %w", sendErr)
}

// sendPayloadToTraceEdge sends a payload to Trace Edge
func (con *traceEdgeConnection) sendPayloadToTraceEdge(ctx context.Context, apiKey string, payload *Payload, url string) (bool, error) {

	// Create the request to be sent to the API
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload.Bytes))

	if err != nil {
		return false, err
	}

	utils.SetDDHeaders(req.Header, con.startInfo, apiKey)
	utils.SetExtraHeaders(req.Header, payload.Headers)

	client := utils.NewHTTPClient(traceEdgeTimeout)
	resp, err := client.Do(req)

	if err != nil {
		// in this case, the payload and client are malformed in some way, so we should not retry
		return false, err
	}
	defer resp.Body.Close()

	// We check the status code to see if the request has succeeded.
	// TODO: define all legit status code and behave accordingly.
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("request to %s responded with %s", url, resp.Status)
		if resp.StatusCode/100 == 5 {
			// 5xx errors are retriable
			return true, err
		}

		// All others aren't
		return false, err
	}

	// Everything went fine
	return false, nil
}
