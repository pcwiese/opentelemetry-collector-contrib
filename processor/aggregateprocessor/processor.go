// Copyright 2019, OpenTelemetry Authors
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

package aggregateprocessor

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/facette/natsort"
	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.uber.org/zap"
)

// aggregatingProcessor is the component that fowards spans to collector peers
// based on traceID
type aggregatingProcessor struct {
	// to start the memberSyncTicker
	start sync.Once
	// member lock
	lock sync.RWMutex
	// nextConsumer
	nextConsumer consumer.TracesConsumer
	// logger
	logger *zap.Logger
	// self member IP
	selfIP string
	// ringMemberQuerier instance
	ring ringMemberQuerier
	// peer port
	peerPort int
	// ticker to call ring.GetState() and sync member list
	memberSyncTicker tTicker
	// exporters for each of the collector peers
	collectorPeers map[string]component.TracesExporter
}

var _ component.TracesProcessor = (*aggregatingProcessor)(nil)

func newTraceProcessor(logger *zap.Logger, nextConsumer consumer.TracesConsumer, cfg *Config) (component.TracesProcessor, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	if cfg.PeerDiscoveryDNSName == "" {
		return nil, fmt.Errorf("peer discovery DNS name not provided")
	}

	if cfg.PeerPort == 0 {
		return nil, fmt.Errorf("nil peer port")
	}

	ap := &aggregatingProcessor{
		nextConsumer:   nextConsumer,
		logger:         logger,
		collectorPeers: make(map[string]component.TracesExporter),
		peerPort:       cfg.PeerPort,
	}

	ap.ring = newRingMemberQuerier(logger, cfg.PeerDiscoveryDNSName)

	if ip, err := externalIP(); err == nil {
		ap.selfIP = ip
	} else {
		return nil, err
	}

	ap.memberSyncTicker = &policyTicker{onTick: ap.memberSyncOnTick}

	return ap, nil
}

func newTraceExporter(logger *zap.Logger, ip string, peerPort int) component.TracesExporter {
	factory := otlpexporter.NewFactory()
	otlpConfig := &otlpexporter.Config{
		ExporterSettings: configmodels.ExporterSettings{
			NameVal: "otlp",
			TypeVal: configmodels.Type("otlp"),
		},
		GRPCClientSettings: configgrpc.GRPCClientSettings{
			Endpoint: ip + ":" + strconv.Itoa(peerPort),
			TLSSetting: configtls.TLSClientSetting{
				Insecure: true,
			},
		},
	}

	logger.Info("Creating new exporter for", zap.String("PeerIP", ip), zap.Int("PeerPort", peerPort))
	params := component.ExporterCreateParams{Logger: logger}
	exporter, err := factory.CreateTracesExporter(context.Background(), params, otlpConfig)
	if err != nil {
		logger.Fatal("Could not create span exporter", zap.Error(err))
		return nil
	}

	return exporter
}

// At the set frequency, get the state of the collector peer list
func (ap *aggregatingProcessor) memberSyncOnTick() {
	newMembers := ap.ring.getMembers()
	if newMembers == nil {
		return
	}

	// check if member list has changed
	ap.lock.RLock()
	curMembers := make([]string, 0, len(ap.collectorPeers))
	for k := range ap.collectorPeers {
		curMembers = append(curMembers, k)
	}
	ap.lock.RUnlock()

	natsort.Sort(curMembers)
	natsort.Sort(newMembers)

	// checking if curMembers == newMembers
	isEqual := true
	if len(curMembers) != len(newMembers) {
		isEqual = false
	} else {
		for k, v := range curMembers {
			if v != newMembers[k] {
				isEqual = false
			}
		}
	}

	if !isEqual {
		// Remove old members
		// Find diff(curMembers, newMembers)
		for _, c := range curMembers {
			// check if c is part of newMembers
			flag := 0
			for _, n := range newMembers {
				if c == n {
					flag = 1
				}
			}
			if flag == 0 {
				// Need a write lock here
				ap.lock.Lock()
				// nullify the collector peer instance
				ap.collectorPeers[c] = nil
				// delete the key
				delete(ap.collectorPeers, c)
				ap.lock.Unlock()
				ap.logger.Info("(memberSyncOnTick) Deleted member", zap.String("Member ip", c))
			}
		}
		// Add new members
		for _, v := range newMembers {
			if _, ok := ap.collectorPeers[v]; ok {
				// exists, do nothing
			} else if v == ap.selfIP {
				// Need a write lock here
				ap.lock.Lock()
				ap.collectorPeers[v] = nil
				ap.lock.Unlock()
			} else {
				newPeer := newTraceExporter(ap.logger, v, ap.peerPort)
				if newPeer == nil {
					return
				}
				// Need a write lock here
				ap.lock.Lock()
				// build a new trace exporter
				ap.collectorPeers[v] = newPeer
				ap.lock.Unlock()
				ap.logger.Info("(memberSyncOnTick) Added member", zap.String("Member ip", v))
			}
		}
	}
}

func externalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("Are you connected to the network?")
}

// Copied from github.com/dgryski/go-jump/blob/master/jump.go
func jumpHash(key uint64, numBuckets int) int32 {

	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

func fingerprint(b []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(b)
	return hash.Sum64()
}

func prepareTraceBatch(spans []*pdata.Span) pdata.Traces {
	traceTd := pdata.NewTraces()
	traceTd.ResourceSpans().Resize(1)
	rs := traceTd.ResourceSpans().At(0)
	rs.Resource().InitEmpty()
	rs.InstrumentationLibrarySpans().Resize(1)
	ils := rs.InstrumentationLibrarySpans().At(0)
	for _, span := range spans {
		ils.Spans().Append(*span)
	}
	return traceTd
}

func (ap *aggregatingProcessor) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	// Start member sync
	ap.start.Do(func() {
		ap.logger.Info("First span received, starting member sync timer")
		// Run first one manually
		ap.memberSyncOnTick()
		ap.memberSyncTicker.Start(10000 * time.Millisecond) // 10s
	})

	if td.SpanCount() == 0 {
		return fmt.Errorf("Empty batch")
	}

	stats.Record(ctx, statCountSpansReceived.M(int64(td.SpanCount())))

	ap.lock.RLock()
	defer ap.lock.RUnlock()

	// get members:
	peersSorted := []string{}
	for k := range ap.collectorPeers {
		peersSorted = append(peersSorted, k)
	}

	natsort.Sort(peersSorted)
	ap.logger.Debug("Printing member list", zap.Strings("Members", peersSorted))

	// build batches for every member
	var batches [][]*pdata.Span
	for p := 0; p < len(peersSorted); p++ {
		batches = append(batches, make([]*pdata.Span, 0))
	}

	// Should ideally be empty
	noHashBatch := []*pdata.Span{}

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourcespan := td.ResourceSpans().At(i)
		if resourcespan.IsNil() {
			ap.logger.Debug("Empty resource span. Skipping")
			continue
		}

		for j := 0; j < resourcespan.InstrumentationLibrarySpans().Len(); j++ {
			ilspan := resourcespan.InstrumentationLibrarySpans().At(j)
			if ilspan.IsNil() {
				ap.logger.Debug("Empty instrumentation library span. Skipping")
				continue
			}

			for k := 0; k < ilspan.Spans().Len(); k++ {
				span := ilspan.Spans().At(k)
				if span.IsNil() {
					ap.logger.Debug("Empty span. Skipping")
					continue
				}

				traceIDBytes := span.TraceID().Bytes()
				memberNum := jumpHash(fingerprint(traceIDBytes[:]), len(peersSorted))
				if memberNum == -1 {
					// Any spans having a hash error -> self processed
					ap.logger.Debug("Adding span to self due to hash error", zap.String("TraceID", span.TraceID().HexString()))
					noHashBatch = append(noHashBatch, &span)
				} else {
					// Append this span to the batch of that member
					curIP := peersSorted[memberNum]
					if curIP == ap.selfIP {
						ap.logger.Debug("Adding span to self", zap.String("TraceID", span.TraceID().HexString()))
					} else {
						ap.logger.Debug("Adding span to peer", zap.String("TraceID", span.TraceID().HexString()), zap.String("PeerIP", curIP))
					}

					batches[memberNum] = append(batches[memberNum], &span)
				}
			}
		}
	}

	for k, batch := range batches {
		if len(batch) == 0 {
			ap.logger.Debug("Empty batch. Skipping")
			continue
		}

		curIP := peersSorted[k]
		if curIP == ap.selfIP {
			batch = append(batch, noHashBatch...)
			toSend := prepareTraceBatch(batch)
			ap.logger.Debug("Processing batch locally", zap.Int("Batch-size", len(batch)))
			err := ap.nextConsumer.ConsumeTraces(ctx, toSend)
			if err != nil {
				ap.logger.Error("Error consuming trace locally", zap.Error(err))
				return err
			}

			continue
		} else {
			// track metrics for forwarded spans
			stats.Record(ctx, statCountSpansForwarded.M(int64(len(batch))))
		}

		ap.logger.Debug("Sending batch to peer", zap.String("PeerIP", curIP), zap.Int("Batch-size", len(batch)))
		toSend := prepareTraceBatch(batch)
		err := ap.collectorPeers[curIP].ConsumeTraces(ctx, toSend)
		if err != nil {
			ap.logger.Error("Error consuming trace on peer", zap.String("PeerIP", curIP), zap.Error(err))
			return err
		}
	}

	return nil
}

func (ap *aggregatingProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

// Start is invoked during service startup.
func (ap *aggregatingProcessor) Start(context.Context, component.Host) error {
	return nil
}

// Shutdown is invoked during service shutdown.
func (ap *aggregatingProcessor) Shutdown(_ context.Context) error {
	ap.memberSyncTicker.Stop()
	return nil
}

// tTicker interface allows easier testing of ticker related functionality used by tailSamplingProcessor
type tTicker interface {
	// Start sets the frequency of the ticker and starts the periodic calls to OnTick.
	Start(d time.Duration)
	// OnTick is called when the ticker fires.
	OnTick()
	// Stops firing the ticker.
	Stop()
}

type policyTicker struct {
	ticker *time.Ticker
	onTick func()
}

func (pt *policyTicker) Start(d time.Duration) {
	pt.ticker = time.NewTicker(d)
	go func() {
		for range pt.ticker.C {
			pt.OnTick()
		}
	}()
}
func (pt *policyTicker) OnTick() {
	pt.onTick()
}
func (pt *policyTicker) Stop() {
	pt.ticker.Stop()
}

var _ tTicker = (*policyTicker)(nil)
