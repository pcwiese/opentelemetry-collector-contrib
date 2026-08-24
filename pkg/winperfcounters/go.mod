module github.com/open-telemetry/opentelemetry-collector-contrib/pkg/winperfcounters

go 1.20

require (
	github.com/stretchr/testify v1.12.1
	golang.org/x/sys v0.13.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
