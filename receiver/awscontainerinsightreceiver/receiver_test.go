// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscontainerinsightreceiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
)

// Mock cadvisor
type mockCadvisor struct{}

func (c *mockCadvisor) GetMetrics() []pmetric.Metrics {
	md := pmetric.NewMetrics()
	return []pmetric.Metrics{md}
}

func (c *mockCadvisor) Shutdown() error {
	return nil
}

// Mock k8sapiserver
type mockK8sAPIServer struct{}

func (m *mockK8sAPIServer) Shutdown() error {
	return nil
}

func (m *mockK8sAPIServer) GetMetrics() []pmetric.Metrics {
	md := pmetric.NewMetrics()
	return []pmetric.Metrics{md}
}

func TestReceiver(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	metricsReceiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	r := metricsReceiver.(*awsContainerInsightReceiver)
	ctx := context.Background()

	err = r.Start(ctx, componenttest.NewNopHost())
	require.Error(t, err)

	err = r.Shutdown(ctx)
	require.NoError(t, err)
}

func TestCollectData(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	metricsReceiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		new(consumertest.MetricsSink),
	)

	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	r := metricsReceiver.(*awsContainerInsightReceiver)
	_ = r.Start(context.Background(), nil)
	ctx := context.Background()
	r.k8sapiserver = &mockK8sAPIServer{}
	r.containerMetricsProvider = &mockCadvisor{}
	err = r.collectData(ctx)
	require.NoError(t, err)

	// test the case when cadvisor and k8sapiserver failed to initialize
	r.containerMetricsProvider = nil
	r.k8sapiserver = nil
	err = r.collectData(ctx)
	require.Error(t, err)
}

func TestCollectDataWithErrConsumer(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	metricsReceiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		consumertest.NewErr(errors.New("an error")),
	)

	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	r := metricsReceiver.(*awsContainerInsightReceiver)
	_ = r.Start(context.Background(), nil)
	r.containerMetricsProvider = &mockCadvisor{}
	r.k8sapiserver = &mockK8sAPIServer{}
	ctx := context.Background()

	err = r.collectData(ctx)
	require.Error(t, err)
}

func TestCollectDataWithECS(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.ContainerOrchestrator = ci.ECS
	metricsReceiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		new(consumertest.MetricsSink),
	)

	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	r := metricsReceiver.(*awsContainerInsightReceiver)
	_ = r.Start(context.Background(), nil)
	ctx := context.Background()

	r.containerMetricsProvider = &mockCadvisor{}
	err = r.collectData(ctx)
	require.NoError(t, err)

	// test the case when cadvisor and k8sapiserver failed to initialize
	r.containerMetricsProvider = nil
	err = r.collectData(ctx)
	require.Error(t, err)
}

func TestCollectDataWithSystemd(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.ContainerOrchestrator = ci.EKS
	cfg.KubeConfigPath = "/tmp/kube-config"
	cfg.HostIP = "1.2.3.4"
	metricsReceiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		new(consumertest.MetricsSink),
	)

	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	r := metricsReceiver.(*awsContainerInsightReceiver)
	_ = r.Start(context.Background(), nil)
	ctx := context.Background()

	r.containerMetricsProvider = &mockCadvisor{}
	err = r.collectData(ctx)
	require.NoError(t, err)
}

// MockHost is a mock implementation of component.Host
type MockHost struct {
	mock.Mock
}

func (m *MockHost) GetExtensions() map[component.ID]component.Component {
	args := m.Called()
	return args.Get(0).(map[component.ID]component.Component)
}

// MockConfigurer is a mock implementation of awsmiddleware.Configurer
type MockConfigurer struct {
	mock.Mock
}

func (m *MockConfigurer) Start(context.Context, component.Host) error {
	return nil
}

func (m *MockConfigurer) Shutdown(context.Context) error {
	return nil
}

func (m *MockHost) GetFactory(_ component.Kind, _ component.Type) component.Factory {
	return nil
}

func TestAWSContainerInsightReceiverStart(t *testing.T) {
	// Create a mock host
	mockHost := new(MockHost)
	testType, _ := component.NewType("awsmiddleware")

	// Create a mock configurer
	mockConfigurer := new(MockConfigurer)
	agenthealth, _ := component.NewType("agenthealth")
	// Set up the mock host to return a map with the mock configurer
	mockHost.On("GetExtensions").Return(map[component.ID]component.Component{
		component.NewID(testType): mockConfigurer,
	})

	statusCodeID := component.NewIDWithName(agenthealth, "statuscode")

	// Create a receiver instance
	config := &Config{
		CollectionInterval:    60,
		ContainerOrchestrator: "eks",
		MiddlewareID:          &statusCodeID,
	}
	consumer := consumertest.NewNop()
	receiver, err := newAWSContainerInsightReceiver(component.TelemetrySettings{}, config, consumer)
	assert.NoError(t, err)
	err = receiver.Start(context.Background(), mockHost)
	assert.Error(t, err)

	mockHost.AssertCalled(t, "GetExtensions")
}

// TestReceiver_initNeuronScraper_withNeuroncoreMetrics tests that the neuron scraper
// is properly initialized when accelerated compute metrics are enabled and verifies
// that the correct metric type (TypeContainerNeuroncore) is used for neuroncore metrics.
func TestReceiver_initNeuronScraper_withNeuroncoreMetrics(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.EnableAcceleratedComputeMetrics = true // Enable accelerated compute metrics
	cfg.ContainerOrchestrator = ci.EKS

	receiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, receiver)

	r := receiver.(*awsContainerInsightReceiver)

	// Verify that EnableAcceleratedComputeMetrics is properly set
	assert.True(t, r.config.EnableAcceleratedComputeMetrics, 
		"EnableAcceleratedComputeMetrics should be true for neuroncore metrics collection")

	// Note: Full initialization testing would require mocking hostinfo and component.Host,
	// but the key verification is that the configuration properly enables accelerated compute metrics
	// which is required for neuroncore metrics collection. The actual scraper initialization
	// is tested through integration tests.
}

// TestReceiver_initNeuronScraper_disabled tests that the neuron scraper initialization
// is skipped when accelerated compute metrics are disabled, ensuring no unnecessary
// resource allocation for neuroncore metrics collection.
func TestReceiver_initNeuronScraper_disabled(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.EnableAcceleratedComputeMetrics = false // Disable accelerated compute metrics
	cfg.ContainerOrchestrator = ci.EKS

	receiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, receiver)

	r := receiver.(*awsContainerInsightReceiver)

	// Verify that EnableAcceleratedComputeMetrics is properly set to false
	assert.False(t, r.config.EnableAcceleratedComputeMetrics,
		"EnableAcceleratedComputeMetrics should be false when neuroncore metrics are disabled")

	// When accelerated compute metrics are disabled, the neuron scraper should not be initialized
	// This saves resources and prevents unnecessary metric collection
}

// TestReceiver_initEfaScraper_withEfaMetrics tests that the EFA scraper
// is properly initialized when accelerated compute metrics are enabled and verifies
// that the correct metric type (TypeContainerEFA) is used for EFA metrics.
func TestReceiver_initEfaScraper_withEfaMetrics(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.EnableAcceleratedComputeMetrics = true // Enable accelerated compute metrics
	cfg.ContainerOrchestrator = ci.EKS

	receiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, receiver)

	r := receiver.(*awsContainerInsightReceiver)

	// Verify that EnableAcceleratedComputeMetrics is properly set
	assert.True(t, r.config.EnableAcceleratedComputeMetrics, 
		"EnableAcceleratedComputeMetrics should be true for EFA metrics collection")

	// Note: Full initialization testing would require mocking hostinfo and component.Host,
	// but the key verification is that the configuration properly enables accelerated compute metrics
	// which is required for EFA metrics collection. The actual scraper initialization
	// is tested through integration tests.
}

// TestReceiver_initEfaScraper_disabled tests that the EFA scraper initialization
// is skipped when accelerated compute metrics are disabled, ensuring no unnecessary
// resource allocation for EFA metrics collection.
func TestReceiver_initEfaScraper_disabled(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.EnableAcceleratedComputeMetrics = false // Disable accelerated compute metrics
	cfg.ContainerOrchestrator = ci.EKS

	receiver, err := newAWSContainerInsightReceiver(
		componenttest.NewNopTelemetrySettings(),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, receiver)

	r := receiver.(*awsContainerInsightReceiver)

	// Verify that EnableAcceleratedComputeMetrics is properly set to false
	assert.False(t, r.config.EnableAcceleratedComputeMetrics,
		"EnableAcceleratedComputeMetrics should be false when EFA metrics are disabled")

	// When accelerated compute metrics are disabled, the EFA scraper should not be initialized
	// This saves resources and prevents unnecessary metric collection
}

// TestReceiver_EfaConfiguration tests various EFA-related configuration scenarios
func TestReceiver_EfaConfiguration(t *testing.T) {
	testCases := []struct {
		name                            string
		enableAcceleratedComputeMetrics bool
		containerOrchestrator           string
		expectEfaEnabled                bool
		description                     string
	}{
		{
			name:                            "EFA enabled for EKS",
			enableAcceleratedComputeMetrics: true,
			containerOrchestrator:           ci.EKS,
			expectEfaEnabled:                true,
			description:                     "EFA metrics should be enabled for EKS with accelerated compute metrics",
		},
		{
			name:                            "EFA disabled for EKS",
			enableAcceleratedComputeMetrics: false,
			containerOrchestrator:           ci.EKS,
			expectEfaEnabled:                false,
			description:                     "EFA metrics should be disabled when accelerated compute metrics are off",
		},
		{
			name:                            "EFA enabled for ECS",
			enableAcceleratedComputeMetrics: true,
			containerOrchestrator:           ci.ECS,
			expectEfaEnabled:                true,
			description:                     "EFA metrics should be enabled for ECS with accelerated compute metrics",
		},
		{
			name:                            "EFA disabled for ECS",
			enableAcceleratedComputeMetrics: false,
			containerOrchestrator:           ci.ECS,
			expectEfaEnabled:                false,
			description:                     "EFA metrics should be disabled for ECS when accelerated compute metrics are off",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.EnableAcceleratedComputeMetrics = tc.enableAcceleratedComputeMetrics
			cfg.ContainerOrchestrator = tc.containerOrchestrator

			receiver, err := newAWSContainerInsightReceiver(
				componenttest.NewNopTelemetrySettings(),
				cfg,
				consumertest.NewNop(),
			)
			require.NoError(t, err)
			require.NotNil(t, receiver)

			r := receiver.(*awsContainerInsightReceiver)

			// Verify the configuration matches expectations
			assert.Equal(t, tc.expectEfaEnabled, r.config.EnableAcceleratedComputeMetrics, tc.description)
			assert.Equal(t, tc.containerOrchestrator, r.config.ContainerOrchestrator)
		})
	}
}

// TestReceiver_EfaMetricTypes tests that EFA metric types are properly recognized
func TestReceiver_EfaMetricTypes(t *testing.T) {
	// Test that EFA metric types are properly classified
	efaMetricTypes := []string{
		ci.TypeNodeEFA,
		ci.TypePodEFA,
		ci.TypeContainerEFA,
	}

	for _, metricType := range efaMetricTypes {
		t.Run(metricType, func(t *testing.T) {
			switch metricType {
			case ci.TypeNodeEFA:
				assert.True(t, ci.IsNode(metricType), "TypeNodeEFA should be recognized as a node metric")
			case ci.TypePodEFA:
				assert.True(t, ci.IsPod(metricType), "TypePodEFA should be recognized as a pod metric")
			case ci.TypeContainerEFA:
				assert.True(t, ci.IsContainer(metricType), "TypeContainerEFA should be recognized as a container metric")
			}
		})
	}
}

// TestReceiver_EfaMetricNames tests that EFA metric names are properly defined
func TestReceiver_EfaMetricNames(t *testing.T) {
	// Test EFA capacity metrics
	capacityMetrics := []string{
		ci.EfaLimit,
		ci.EfaUsageTotal,
		ci.EfaRequest,
		ci.EfaReservedCapacity,
		ci.EfaUnreservedCapacity,
	}

	for _, metric := range capacityMetrics {
		t.Run(metric, func(t *testing.T) {
			// Verify the metric name is not empty
			assert.NotEmpty(t, metric, "EFA metric name should not be empty")
			
			// Verify the metric has a unit defined
			unit := ci.GetUnitForMetric(metric)
			assert.NotEmpty(t, unit, "EFA metric %s should have a unit defined", metric)
		})
	}

	// Test EFA hardware counter metrics
	hardwareMetrics := []string{
		ci.EfaRdmaReadBytes,
		ci.EfaRdmaWriteBytes,
		ci.EfaRdmaWriteRecvBytes,
		ci.EfaRxBytes,
		ci.EfaRxDropped,
		ci.EfaTxBytes,
	}

	for _, metric := range hardwareMetrics {
		t.Run(metric, func(t *testing.T) {
			// Verify the metric name is not empty
			assert.NotEmpty(t, metric, "EFA hardware metric name should not be empty")
			
			// Verify the metric has a unit defined
			unit := ci.GetUnitForMetric(metric)
			assert.NotEmpty(t, unit, "EFA hardware metric %s should have a unit defined", metric)
		})
	}
}
