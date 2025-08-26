module github.com/open-telemetry/opentelemetry-collector-contrib/extension/httpforwarder

go 1.24

require (
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.88.0
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/confighttp v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/configopaque v1.39.0
	go.opentelemetry.io/collector/config/configtls v1.39.0
	go.opentelemetry.io/collector/confmap v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/extension v0.88.1-0.20231026220224-6405e152a2d9
	go.uber.org/zap v1.26.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20250323135004-b31fac66206e // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.0.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/cors v1.10.1 // indirect
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configauth v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configcompression v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/internal v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/extension/auth v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/featuregate v1.0.0-rcv0017.0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/pdata v1.0.0-rcv0017.0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.45.0 // indirect
	go.opentelemetry.io/otel v1.19.0 // indirect
	go.opentelemetry.io/otel/metric v1.19.0 // indirect
	go.opentelemetry.io/otel/trace v1.19.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.35.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect
	google.golang.org/grpc v1.59.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
