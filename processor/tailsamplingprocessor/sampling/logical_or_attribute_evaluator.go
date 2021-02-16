// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sampling

import (
	"fmt"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

// SingleAttributeEvaluatorCfg defines either a numeric or string evaluator, but not both.
// Reusing existing configuration for the string and numeric attribute configuration.
type SingleAttributeEvaluatorCfg struct {
	// Configs for numeric attribute filter sampling policy evaluator.
	NumericAttributeCfg NumericAttributeEvaluatorCfg `mapstructure:"numeric_attribute"`
	// Configs for string attribute filter sampling policy evaluator.
	StringAttributeCfg StringAttributeEvaluatorCfg `mapstructure:"string_attribute"`
}

// NumericAttributeEvaluatorCfg holds the configurable settings to create a numeric attribute evaluator
type NumericAttributeEvaluatorCfg struct {
	// Tag that the filter is going to be matching against.
	Key string `mapstructure:"key"`
	// MinValue is the minimum value of the attribute to be considered a match.
	MinValue int64 `mapstructure:"min_value"`
	// MaxValue is the maximum value of the attribute to be considered a match.
	MaxValue int64 `mapstructure:"max_value"`
}

// StringAttributeEvaluatorCfg holds the configurable settings to create a string attribute evaluator
type StringAttributeEvaluatorCfg struct {
	// Tag that the filter is going to be matching against.
	Key string `mapstructure:"key"`
	// Values is the set of attribute values that if any is equal to the actual attribute value to be considered a match.
	Values []string `mapstructure:"values"`
	// Specifies the final sampling decision on match. Defaults to sampled.
	// Valid values are: sampled, notsampledfinal
	DecisionOnMatch string `mapstructure:"decision_on_match"`
}

type logicalOrAttributeEvaluator struct {
	logger             *zap.Logger
	keyToEvaluatorsMap map[string][]attributeEvaluator
}

type stringAttributeEvaluator struct {
	key             string
	values          map[string]struct{}
	decisionOnMatch Decision
	logger          *zap.Logger
}

type numericAttributeEvaluator struct {
	key                string
	minValue, maxValue int64
	logger             *zap.Logger
}

type attributeEvaluator interface {
	isMatch(value pdata.AttributeValue) (Decision, error)
	attributeKey() string
}

func newLogicalOrAttributeEvaluator(logger *zap.Logger, evaluators []attributeEvaluator) PolicyEvaluator {
	// Build up a map where the key is the attribute name and value is an array of evaluators for that attribute value.
	// For each attribute iterated over, the key is used to locate evaluators.
	// Worst case, there will be n lookups in this map followed by 1 or more evaluations against the attribute value.
	keyToEvaluatorsMap := make(map[string][]attributeEvaluator)
	for _, evaluator := range evaluators {
		key := evaluator.attributeKey()
		var keySamplers []attributeEvaluator

		if k, ok := keyToEvaluatorsMap[key]; !ok {
			keySamplers = make([]attributeEvaluator, 0)
		} else {
			keySamplers = k
		}

		keyToEvaluatorsMap[key] = append(keySamplers, evaluator)
	}

	return &logicalOrAttributeEvaluator{
		keyToEvaluatorsMap: keyToEvaluatorsMap,
		logger:             logger,
	}
}

// NewLogicalOrAttributeEvaluator creates a policy evaluator that samples traces based on one or more attribute evaluators OR'd together
func NewLogicalOrAttributeEvaluator(logger *zap.Logger, evaluatorsConfig []SingleAttributeEvaluatorCfg) (PolicyEvaluator, error) {
	// Build the evaluators from config
	evaluators := make([]attributeEvaluator, len(evaluatorsConfig))
	for i, evaluatorCfg := range evaluatorsConfig {
		var evaluator attributeEvaluator = nil
		if evaluatorCfg.StringAttributeCfg.Key != "" {
			stringEvaluatorCfg := evaluatorCfg.StringAttributeCfg

			const sampled = "sampled"
			const notsampledfinal = "notsampledfinal"
			decisionOnMatchString := stringEvaluatorCfg.DecisionOnMatch

			// Validate the decision on match field value
			if !(decisionOnMatchString == "" || decisionOnMatchString == sampled || decisionOnMatchString == notsampledfinal) {
				return nil, fmt.Errorf("Unknown decision_on_match value %s. Valid values are unspecified or '%s' or '%s'", decisionOnMatchString, sampled, notsampledfinal)
			}

			// If not specified, defaults to sampled
			decisionOnMatch := Sampled
			if decisionOnMatchString == notsampledfinal {
				decisionOnMatch = NotSampledFinal
			}

			evaluator = newStringAttributeEvaluator(logger, stringEvaluatorCfg.Key, stringEvaluatorCfg.Values, decisionOnMatch)
		} else {
			numericEvaluatorCfg := evaluatorCfg.NumericAttributeCfg
			evaluator = newNumericAttributeEvaluator(logger, numericEvaluatorCfg.Key, numericEvaluatorCfg.MinValue, numericEvaluatorCfg.MaxValue)
		}

		evaluators[i] = evaluator
	}

	return newLogicalOrAttributeEvaluator(logger, evaluators), nil
}

// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (ae *logicalOrAttributeEvaluator) OnLateArrivingSpans(Decision, []*pdata.Span) error {
	return nil
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (ae *logicalOrAttributeEvaluator) Evaluate(_ pdata.TraceID, trace *TraceData) (Decision, error) {
	ae.logger.Debug("Evaluating spans in logical OR attribute evaluator")
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()

	decision := NotSampled
	finalDecision := false

	// This function is potentially run for each attribute
	attributeEvaluator := func(key string, value pdata.AttributeValue) {
		// Short-circuit if a final decision was already made.
		if finalDecision {
			return
		}

		// Try the individual evaluators corresponding to the key if it exists.
		if evaluators, ok := ae.keyToEvaluatorsMap[key]; ok {
			for i := range evaluators {
				evaluatorDecision, _ := evaluators[i].isMatch(value)

				if evaluatorDecision == NotSampledFinal || evaluatorDecision == Sampled {
					finalDecision = true
					decision = evaluatorDecision
					ae.logger.Debug("Final decision from attribute value match", zap.String("key", key), zap.String("decision", decision.String()))
					break
				}
			}
		}
	}

	for _, batch := range batches {
		rspans := batch.ResourceSpans()

		for i := 0; i < rspans.Len(); i++ {
			rs := rspans.At(i)
			resource := rs.Resource()
			resource.Attributes().ForEach(attributeEvaluator)

			if finalDecision {
				break
			}

			ilss := rs.InstrumentationLibrarySpans()
			for j := 0; j < ilss.Len(); j++ {
				ils := ilss.At(j)
				for k := 0; k < ils.Spans().Len(); k++ {
					span := ils.Spans().At(k)
					span.Attributes().ForEach(attributeEvaluator)
				}
			}
		}

		if finalDecision {
			break
		}
	}

	return decision, nil
}

func newStringAttributeEvaluator(logger *zap.Logger, key string, values []string, decisionOnMatch Decision) attributeEvaluator {
	valuesMap := make(map[string]struct{})
	for _, value := range values {
		if value != "" {
			valuesMap[value] = struct{}{}
		}
	}
	return &stringAttributeEvaluator{
		key:             key,
		values:          valuesMap,
		decisionOnMatch: decisionOnMatch,
		logger:          logger,
	}
}

func (sae *stringAttributeEvaluator) attributeKey() string {
	return sae.key
}

func (sae *stringAttributeEvaluator) isMatch(value pdata.AttributeValue) (Decision, error) {
	if _, ok := sae.values[value.StringVal()]; ok {
		return sae.decisionOnMatch, nil
	}

	return NotSampled, nil
}

func newNumericAttributeEvaluator(logger *zap.Logger, key string, minValue, maxValue int64) attributeEvaluator {
	return &numericAttributeEvaluator{
		key:      key,
		minValue: minValue,
		maxValue: maxValue,
		logger:   logger,
	}
}

func (nae *numericAttributeEvaluator) attributeKey() string {
	return nae.key
}

func (nae *numericAttributeEvaluator) isMatch(value pdata.AttributeValue) (Decision, error) {
	val := value.IntVal()
	if val >= nae.minValue && val <= nae.maxValue {
		return Sampled, nil
	}

	return NotSampled, nil
}
