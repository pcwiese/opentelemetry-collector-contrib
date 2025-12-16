module github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension

go 1.24.0

require (
	github.com/stretchr/testify v1.11.1
	github.com/tg123/go-htpasswd v1.2.1
	go.opentelemetry.io/collector v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/component v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/config/configopaque v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/confmap v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/extension v0.88.1-0.20231026220224-6405e152a2d9
	go.opentelemetry.io/collector/extension/auth v0.88.1-0.20231026220224-6405e152a2d9
	google.golang.org/grpc v1.77.0
)

require (
	github.com/GehirnInc/crypt v0.0.0-20200316065508-bb7000b8a962 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.0.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.88.1-0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/featuregate v1.0.0-rcv0017.0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/collector/pdata v1.0.0-rcv0017.0.20231026220224-6405e152a2d9 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/crypto v0.43.0 // indirect
	golang.org/x/net v0.46.1-0.20251013234738-63d1a5100f82 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251022142026-3a174f9686a8 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
