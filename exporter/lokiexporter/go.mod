module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/lokiexporter

go 1.24.0

require (
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/grafana/loki/pkg/push v0.0.0-20230127072203-4e8cc8d71928
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/loki v0.88.0
	github.com/prometheus/common v0.45.0
	github.com/stretchr/testify v1.11.1
	go.opencensus.io v0.24.0
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/component v1.45.0
	go.opentelemetry.io/collector/component/componenttest v0.139.0
	go.opentelemetry.io/collector/config/confighttp v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/configopaque v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/configtls v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/confmap v1.45.0
	go.opentelemetry.io/collector/consumer/consumererror v0.139.0
	go.opentelemetry.io/collector/exporter v1.45.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.139.0
	go.opentelemetry.io/collector/exporter/exportertest v0.139.0
	go.opentelemetry.io/collector/pdata v1.45.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20221122212121-6b5c0a4cb7fd // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.88.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus v0.88.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.17.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/prometheus/prometheus v0.47.2 // indirect
	github.com/rs/cors v1.10.1 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configcompression v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configoptional v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.45.0 // indirect
	go.opentelemetry.io/collector/config/internal v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.139.0 // indirect
	go.opentelemetry.io/collector/consumer v1.45.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.139.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.139.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.139.0 // indirect
	go.opentelemetry.io/collector/extension v1.45.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.139.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.45.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.139.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.139.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.45.0 // indirect
	go.opentelemetry.io/collector/receiver v1.45.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.139.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.139.0 // indirect
	go.opentelemetry.io/collector/semconv v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.45.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/grpc v1.76.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal => ../../internal/coreinternal

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/loki => ../../pkg/translator/loki

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus => ../../pkg/translator/prometheus

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil => ../../pkg/pdatautil

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest => ../../pkg/pdatatest

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden => ../../pkg/golden
