module github.com/open-telemetry/opentelemetry-collector-contrib/testbed/mockdatareceivers/mockawsxrayreceiver

go 1.24.0

require (
	github.com/gorilla/mux v1.8.0
	go.opentelemetry.io/collector/component v1.51.0
	go.opentelemetry.io/collector/config/configtls v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/consumer v1.51.0
	go.opentelemetry.io/collector/pdata v1.51.0
	go.opentelemetry.io/collector/receiver v1.51.0
	go.opentelemetry.io/collector/receiver/receiverhelper v0.145.0
	go.uber.org/zap v1.27.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configopaque v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/confmap v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.145.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.51.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.145.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.145.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.51.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.145.0 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251222181119-0a764e51fe1b // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
