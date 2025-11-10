module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter

go 1.24.0

require (
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal v0.88.0
	github.com/stretchr/testify v1.11.1
	go.opencensus.io v0.24.0
	go.opentelemetry.io/collector/component v1.45.0
	go.opentelemetry.io/collector/component/componenttest v0.139.0
	go.opentelemetry.io/collector/confmap v1.45.0
	go.opentelemetry.io/collector/consumer v1.45.0
	go.opentelemetry.io/collector/consumer/consumertest v0.139.0
	go.opentelemetry.io/collector/exporter v1.45.0
	go.opentelemetry.io/collector/exporter/exportertest v0.139.0
	go.opentelemetry.io/collector/exporter/otlpexporter v0.139.0
	go.opentelemetry.io/collector/otelcol v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/pdata v1.45.0
	go.opentelemetry.io/collector/semconv v0.88.1-0.20231026220224-6405e152a2d9
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	k8s.io/api v0.28.3
	k8s.io/apimachinery v0.28.3
	k8s.io/client-go v0.28.3
	k8s.io/utils v0.0.0-20230406110748-d93618cff8a2
	sigs.k8s.io/controller-runtime v0.16.3
)

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/evanphx/json-patch v5.6.0+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.6.0 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20250903184740-5d135037bd4d // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-tpm v0.9.6 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.18.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mostynb/go-grpc-compression v1.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.17.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.45.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/prometheus/statsd_exporter v0.22.7 // indirect
	github.com/shirou/gopsutil/v3 v3.23.9 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/spf13/cobra v1.7.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector v0.139.0 // indirect
	go.opentelemetry.io/collector/client v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configauth v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.139.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v1.45.0 // indirect
	go.opentelemetry.io/collector/config/confignet v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configoptional v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.45.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/config/configtls v1.45.0 // indirect
	go.opentelemetry.io/collector/confmap/converter/expandconverter v0.113.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.45.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.45.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/httpprovider v1.45.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/httpsprovider v1.45.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.45.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.139.0 // indirect
	go.opentelemetry.io/collector/connector v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.139.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror/xconsumererror v0.139.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.139.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper v0.139.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper v0.139.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.139.0 // indirect
	go.opentelemetry.io/collector/extension v1.45.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.45.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.139.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.139.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.139.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.45.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.139.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.139.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.139.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.45.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.139.0 // indirect
	go.opentelemetry.io/collector/processor v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/receiver v1.45.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.139.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.139.0 // indirect
	go.opentelemetry.io/collector/service v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.63.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.20.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/bridge/opencensus v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.19.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.19.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.19.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.19.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/term v0.34.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250825161204-c5933d9347a5 // indirect
	google.golang.org/grpc v1.76.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.100.1 // indirect
	k8s.io/kube-openapi v0.0.0-20230717233707-2695361300d9 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal => ../../pkg/batchpersignal

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)

// ambiguous import: found package cloud.google.com/go/compute/metadata in multiple modules
replace cloud.google.com/go v0.65.0 => cloud.google.com/go v0.110.10
