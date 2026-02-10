module github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk

go 1.24.0

require (
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/collector/consumer/consumererror v0.145.0
	go.opentelemetry.io/collector/exporter v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/pdata v1.51.0
	go.opentelemetry.io/collector/semconv v0.88.1-0.20231026220224-6405e152a2d9
	go.uber.org/multierr v1.11.0
)

require (
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.0.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/component v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/confmap v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/consumer v1.51.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.145.0 // indirect
	go.opentelemetry.io/collector/extension v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/featuregate v1.51.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.145.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251222181119-0a764e51fe1b // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
