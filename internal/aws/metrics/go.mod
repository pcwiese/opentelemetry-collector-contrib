module github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/metrics

go 1.20

require (
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/otel v1.19.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
