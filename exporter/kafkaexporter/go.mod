module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter

go 1.24

require (
	github.com/IBM/sarama v1.41.3
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/gogo/protobuf v1.3.2
	github.com/jaegertracing/jaeger v1.48.0
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.88.0
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka v0.88.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger v0.88.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/zipkin v0.88.0
	github.com/openzipkin/zipkin-go v0.4.2
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/collector/component v1.40.0
	go.opentelemetry.io/collector/component/componenttest v0.134.0
	go.opentelemetry.io/collector/config/configtls v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/confmap v1.40.0
	go.opentelemetry.io/collector/consumer v1.40.0
	go.opentelemetry.io/collector/consumer/consumererror v0.134.0
	go.opentelemetry.io/collector/exporter v0.134.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.134.0
	go.opentelemetry.io/collector/exporter/exportertest v0.134.0
	go.opentelemetry.io/collector/pdata v1.40.0
	go.opentelemetry.io/collector/semconv v0.88.1-0.20231026220224-6405e152a2d9
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/apache/thrift v0.19.0 // indirect
	github.com/aws/aws-sdk-go v1.46.7 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/eapache/go-resiliency v1.4.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.2.2 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configopaque v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configoptional v0.134.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.40.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.134.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.134.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.134.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.134.0 // indirect
	go.opentelemetry.io/collector/extension v1.40.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.134.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.40.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.134.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.134.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.134.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.40.0 // indirect
	go.opentelemetry.io/collector/receiver v1.40.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.134.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.134.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.12.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/log v0.13.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/grpc v1.75.0 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal => ../../internal/coreinternal

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka => ../../internal/kafka

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger => ../../pkg/translator/jaeger

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil => ../../pkg/pdatautil

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest => ../../pkg/pdatatest

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/zipkin => ../../pkg/translator/zipkin

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden => ../../pkg/golden
