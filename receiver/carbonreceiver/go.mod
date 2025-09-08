module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/carbonreceiver

go 1.24

require (
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.88.0
	github.com/stretchr/testify v1.10.0
	go.opencensus.io v0.24.0
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/component v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/confignet v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/confmap v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/consumer v1.41.0
	go.opentelemetry.io/collector/consumer/consumertest v0.135.0
	go.opentelemetry.io/collector/pdata v1.41.0
	go.opentelemetry.io/collector/receiver v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/otel/sdk v1.37.0
	go.uber.org/zap v1.26.0
)

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.17.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.45.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/prometheus/statsd_exporter v0.22.7 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.135.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.135.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.41.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.135.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.42.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/grpc v1.75.0 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
