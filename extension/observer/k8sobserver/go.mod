module github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/k8sobserver

go 1.14

require (
	github.com/Azure/go-autorest/autorest/adal v0.9.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer v0.0.0-00010101000000-000000000000
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.6.1
	go.opentelemetry.io/collector v0.9.1-0.20200828041256-df1879c6390f
	go.uber.org/zap v1.15.0
	k8s.io/api v0.19.1
	k8s.io/apimachinery v0.19.1
	k8s.io/client-go v0.19.1
	k8s.io/utils v0.0.0-20200729134348-d5654de09c73
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer => ../

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../../internal/common

// Yet another hack that we need until kubernetes client moves to the new github.com/googleapis/gnostic
replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.3.1
