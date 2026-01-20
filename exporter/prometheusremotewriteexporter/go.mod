module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter

go 1.24.9

require (
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/fsnotify/fsnotify v1.9.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v1.0.0
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.88.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/resourcetotelemetry v0.88.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus v0.88.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite v0.88.0
	github.com/prometheus/prometheus v0.309.1
	github.com/stretchr/testify v1.11.1
	github.com/tidwall/wal v1.1.7
	go.opentelemetry.io/collector/component v1.49.0
	go.opentelemetry.io/collector/component/componenttest v0.143.0
	go.opentelemetry.io/collector/config/confighttp v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/configopaque v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/configtls v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/confmap v1.48.0
	go.opentelemetry.io/collector/consumer/consumererror v0.143.0
	go.opentelemetry.io/collector/exporter v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/pdata v1.49.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.4 // indirect
	github.com/rs/cors v1.10.1 // indirect
	github.com/tidwall/gjson v1.10.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/tinylru v1.1.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configauth v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configcompression v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/internal v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/consumer v1.49.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.143.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.143.0 // indirect
	go.opentelemetry.io/collector/extension v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/extension/auth v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/featuregate v1.49.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.143.0 // indirect
	go.opentelemetry.io/collector/receiver v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/semconv v0.128.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.64.0 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal => ../../internal/coreinternal

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/resourcetotelemetry => ../../pkg/resourcetotelemetry

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus => ../../pkg/translator/prometheus

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite => ../../pkg/translator/prometheusremotewrite

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil => ../../pkg/pdatautil

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest => ../../pkg/pdatatest

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden => ../../pkg/golden
