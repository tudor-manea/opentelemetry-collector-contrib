// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neuron

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/prometheusscraper/decoratorconsumer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
)

var (
	dummyPodName               = "pod-name"
	dummyPodNameForAltResource = "pod-name-alt"
	dummyContainerName         = "container-name"
	dummyNamespace             = "namespace"
)

type mockPodResourcesStore struct{}

func (m mockPodResourcesStore) GetContainerInfo(_ string, _ string) *stores.ContainerInfo {
	return &stores.ContainerInfo{
		PodName:       dummyPodName,
		ContainerName: dummyContainerName,
		Namespace:     dummyNamespace,
	}
}

type mockPodResourcesStoreWithAltResourceName struct{}

func (m mockPodResourcesStoreWithAltResourceName) GetContainerInfo(_ string, resourceName string) *stores.ContainerInfo {
	if resourceName == neuronDeviceResourceNameAlt {
		return &stores.ContainerInfo{
			PodName:       dummyPodNameForAltResource,
			ContainerName: dummyContainerName,
			Namespace:     dummyNamespace,
		}
	}
	return nil
}

func TestConsumeMetricsForPodAttributeDecorator(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dc := &PodAttributesDecoratorConsumer{
		NextConsumer:      consumertest.NewNop(),
		PodResourcesStore: mockPodResourcesStore{},
		Logger:            logger,
	}
	ctx := context.Background()

	testcases1 := map[string]decoratorconsumer.TestCase{
		"empty": {
			Metrics:     pmetric.NewMetrics(),
			Want:        pmetric.NewMetrics(),
			ShouldError: false,
		},
		"neuron_hardware_info_not_found": {
			Metrics: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device": "test0",
					},
				},
			}),

			Want: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device": "test0",
					},
				},
			}),
			ShouldError: false,
		},
		"correlation_via_neuron_device_index": {
			Metrics: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronDeviceAttributeKey: "1",
					},
				},
			}),
			Want: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronDeviceAttributeKey: "1",
						ci.ContainerNamekey:      dummyContainerName,
						ci.K8sPodNameKey:         dummyPodName,
						ci.K8sNamespace:          dummyNamespace,
					},
				},
			}),
			ShouldError: false,
		},
		"correlation_via_neuron_core": {
			Metrics: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":               "test0",
						neuronCoreAttributeKey: "10",
					},
				},
			}),
			Want: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronCoreAttributeKey:   "10",
						neuronDeviceAttributeKey: "5",
						ci.ContainerNamekey:      dummyContainerName,
						ci.K8sPodNameKey:         dummyPodName,
						ci.K8sNamespace:          dummyNamespace,
					},
				},
			}),
			ShouldError: false,
		},
		"correlation_when_both_present": {
			Metrics: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronDeviceAttributeKey: "5",
						neuronCoreAttributeKey:   "10",
					},
				},
			}),
			Want: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronCoreAttributeKey:   "10",
						neuronDeviceAttributeKey: "5",
						ci.ContainerNamekey:      dummyContainerName,
						ci.K8sPodNameKey:         dummyPodName,
						ci.K8sNamespace:          dummyNamespace,
					},
				},
			}),
			ShouldError: false,
		},
	}

	decoratorconsumer.RunDecoratorTestScenarios(ctx, t, dc, testcases1)

	dc = &PodAttributesDecoratorConsumer{
		NextConsumer:      consumertest.NewNop(),
		PodResourcesStore: mockPodResourcesStoreWithAltResourceName{},
		Logger:            logger,
	}

	testcases2 := map[string]decoratorconsumer.TestCase{
		"correlation_via_neuron_device_index_alt_name": {
			Metrics: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronDeviceAttributeKey: "1",
					},
				},
			}),
			Want: decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{
						neuronCorePerDeviceKey: "2",
					},
				},
				{Name: "test", MetricType: pmetric.MetricTypeGauge}: {
					{
						"device":                 "test0",
						neuronDeviceAttributeKey: "1",
						ci.ContainerNamekey:      dummyContainerName,
						ci.K8sPodNameKey:         dummyPodNameForAltResource,
						ci.K8sNamespace:          dummyNamespace,
					},
				},
			}),
			ShouldError: false,
		},
	}

	decoratorconsumer.RunDecoratorTestScenarios(ctx, t, dc, testcases2)
}

func TestNeuronDeviceResourceNameCompatibility(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	testCases := []struct {
		name        string
		neuronKey   string
		storeMock   PodResourcesStoreInterface
		expectedPod string
	}{
		{
			name:        "neuroncore_resource_key",
			neuronKey:   "aws.amazon.com/neuroncore",
			storeMock:   &mockNeuronCoreStore{},
			expectedPod: dummyPodName,
		},
		{
			name:        "neuron_resource_key_fallback",
			neuronKey:   "aws.amazon.com/neuron",
			storeMock:   &mockNeuronFallbackStore{},
			expectedPod: dummyPodNameForAltResource,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dc := &PodAttributesDecoratorConsumer{
				NextConsumer:      consumertest.NewNop(),
				PodResourcesStore: tc.storeMock,
				Logger:            logger,
			}

			metrics := decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{neuronCorePerDeviceKey: "2"},
				},
				{Name: "test_metric", MetricType: pmetric.MetricTypeGauge}: {
					{neuronCoreAttributeKey: "1"},
				},
			})

			err := dc.ConsumeMetrics(ctx, metrics)
			if err != nil {
				t.Errorf("ConsumeMetrics failed for %s: %v", tc.neuronKey, err)
			}

			// Verify metrics were processed and correct pod attributes were added
			found := false
			rms := metrics.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				ilms := rms.At(i).ScopeMetrics()
				for j := 0; j < ilms.Len(); j++ {
					metricSlice := ilms.At(j).Metrics()
					for k := 0; k < metricSlice.Len(); k++ {
						metric := metricSlice.At(k)
						if metric.Name() == "test_metric" {
							datapoints := getMetricDatapoints(metric)
							if datapoints.Len() > 0 {
								attrs := datapoints.At(0).Attributes()
								if podName, exists := attrs.Get(ci.K8sPodNameKey); exists {
									if podName.AsString() == tc.expectedPod {
										found = true
									}
								}
							}
						}
					}
				}
			}
			if !found {
				t.Errorf("Expected pod name %s not found for %s", tc.expectedPod, tc.neuronKey)
			}
		})
	}
}

type mockNeuronCoreStore struct{}

func (m *mockNeuronCoreStore) GetContainerInfo(_ string, resourceName string) *stores.ContainerInfo {
	if resourceName == neuronCoreResourceName {
		return &stores.ContainerInfo{
			PodName:       dummyPodName,
			ContainerName: dummyContainerName,
			Namespace:     dummyNamespace,
		}
	}
	return nil
}

type mockNeuronFallbackStore struct{}

func (m *mockNeuronFallbackStore) GetContainerInfo(_ string, resourceName string) *stores.ContainerInfo {
	if resourceName == neuronDeviceResourceNameAlt {
		return &stores.ContainerInfo{
			PodName:       dummyPodNameForAltResource,
			ContainerName: dummyContainerName,
			Namespace:     dummyNamespace,
		}
	}
	return nil
}

func TestLNCCoreToDeviceMapping(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	testCases := []struct {
		name                 string
		neuronCoresPerDevice int
		logicalCoreIndex     string
		expectedDeviceIndex  string
		lncMappingActive     bool
	}{
		{
			name:                 "LNC_non_linear_mapping",
			neuronCoresPerDevice: 2,
			logicalCoreIndex:     "5",
			expectedDeviceIndex:  "2",
			lncMappingActive:     true,
		},
		{
			name:                 "Standard_linear_mapping",
			neuronCoresPerDevice: 2,
			logicalCoreIndex:     "4",
			expectedDeviceIndex:  "2",
			lncMappingActive:     false,
		},
		{
			name:                 "LNC_sparse_allocation",
			neuronCoresPerDevice: 4,
			logicalCoreIndex:     "9",
			expectedDeviceIndex:  "2",
			lncMappingActive:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var storeMock PodResourcesStoreInterface
			if tc.lncMappingActive {
				storeMock = &mockLNCPodResourcesStore{}
			} else {
				storeMock = &mockPodResourcesStore{}
			}

			dc := &PodAttributesDecoratorConsumer{
				NextConsumer:      consumertest.NewNop(),
				PodResourcesStore: storeMock,
				Logger:            logger,
			}

			metrics := decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{neuronCorePerDeviceKey: fmt.Sprintf("%d", tc.neuronCoresPerDevice)},
				},
				{Name: "neuron_test_metric", MetricType: pmetric.MetricTypeGauge}: {
					{neuronCoreAttributeKey: tc.logicalCoreIndex},
				},
			})

			err := dc.ConsumeMetrics(ctx, metrics)
			assert.NoError(t, err)

			found := false
			rms := metrics.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				ilms := rms.At(i).ScopeMetrics()
				for j := 0; j < ilms.Len(); j++ {
					metricSlice := ilms.At(j).Metrics()
					for k := 0; k < metricSlice.Len(); k++ {
						metric := metricSlice.At(k)
						if metric.Name() == "neuron_test_metric" {
							datapoints := getMetricDatapoints(metric)
							if datapoints.Len() > 0 {
								attrs := datapoints.At(0).Attributes()
								if deviceIndex, exists := attrs.Get(neuronDeviceAttributeKey); exists {
									assert.Equal(t, tc.expectedDeviceIndex, deviceIndex.AsString(),
										"Device index mapping should work correctly with LNC")
									found = true
								}
							}
						}
					}
				}
			}
			assert.True(t, found, "Should find device index mapping")
		})
	}
}

type mockLNCPodResourcesStore struct{}

func (m *mockLNCPodResourcesStore) GetContainerInfo(_ string, resourceName string) *stores.ContainerInfo {
	if resourceName == neuronCoreResourceName {
		return &stores.ContainerInfo{
			PodName:       dummyPodName,
			ContainerName: dummyContainerName,
			Namespace:     dummyNamespace,
		}
	}
	return nil
}

func TestNeuronCoreWithLNCCompatibility(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	// This test verifies that the PodAttributesDecoratorConsumer correctly handles
	// both neuroncore and neuron resource keys when LNC is configured
	testCases := []struct {
		name            string
		resourceKey     string
		coresPerDevice  int
		expectedPodName string
	}{
		{
			name:            "LNC_with_neuroncore_resource",
			resourceKey:     neuronCoreResourceName,
			coresPerDevice:  4,
			expectedPodName: dummyPodName,
		},
		{
			name:            "LNC_with_neuron_resource",
			resourceKey:     neuronDeviceResourceNameAlt,
			coresPerDevice:  2,
			expectedPodName: dummyPodNameForAltResource,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock store that will return container info based on the resource key
			var storeMock PodResourcesStoreInterface
			if tc.resourceKey == neuronCoreResourceName {
				storeMock = &mockPodResourcesStore{}
			} else {
				storeMock = &mockPodResourcesStoreWithAltResourceName{}
			}

			dc := &PodAttributesDecoratorConsumer{
				NextConsumer:      consumertest.NewNop(),
				PodResourcesStore: storeMock,
				Logger:            logger,
			}

			// Generate metrics with hardware info and a test metric with core attribute
			metrics := decoratorconsumer.GenerateMetrics(map[decoratorconsumer.MetricIdentifier][]map[string]string{
				{Name: neuronHardwareInfoKey, MetricType: pmetric.MetricTypeSum}: {
					{neuronCorePerDeviceKey: fmt.Sprintf("%d", tc.coresPerDevice)},
				},
				{Name: "test_metric", MetricType: pmetric.MetricTypeGauge}: {
					{neuronCoreAttributeKey: "1"},
				},
			})

			// Process the metrics
			err := dc.ConsumeMetrics(ctx, metrics)
			assert.NoError(t, err)

			// Verify pod attributes were added correctly
			found := false
			rms := metrics.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				ilms := rms.At(i).ScopeMetrics()
				for j := 0; j < ilms.Len(); j++ {
					metricSlice := ilms.At(j).Metrics()
					for k := 0; k < metricSlice.Len(); k++ {
						metric := metricSlice.At(k)
						if metric.Name() == "test_metric" {
							datapoints := getMetricDatapoints(metric)
							if datapoints.Len() > 0 {
								attrs := datapoints.At(0).Attributes()
								if podName, exists := attrs.Get(ci.K8sPodNameKey); exists {
									assert.Equal(t, tc.expectedPodName, podName.AsString())
									found = true
								}
								// Verify device index was calculated correctly
								if deviceIndex, exists := attrs.Get(neuronDeviceAttributeKey); exists {
									expectedDeviceIndex := "0" // Core 1 / cores_per_device
									assert.Equal(t, expectedDeviceIndex, deviceIndex.AsString())
								}
							}
						}
					}
				}
			}
			assert.True(t, found, "Should find pod name in decorated metrics")
		})
	}
}

func TestLNCCoreToDeviceIndexMapping(t *testing.T) {
	// This test verifies that the core-to-device index mapping works correctly
	// with different LNC configurations (different cores per device)
	testCases := []struct {
		name              string
		coreIndex         string
		coresPerDevice    int
		expectedDeviceIdx string
	}{
		{
			name:              "standard_mapping_2_cores",
			coreIndex:         "3",
			coresPerDevice:    2,
			expectedDeviceIdx: "1", // 3/2 = 1
		},
		{
			name:              "standard_mapping_4_cores",
			coreIndex:         "6",
			coresPerDevice:    4,
			expectedDeviceIdx: "1", // 6/4 = 1
		},
		{
			name:              "zero_core_index",
			coreIndex:         "0",
			coresPerDevice:    2,
			expectedDeviceIdx: "0", // 0/2 = 0
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val := pcommon.NewValueStr(tc.coreIndex)
			result := getNeuronDeviceIndexFromCoreAttribute(val, tc.coresPerDevice)
			assert.Equal(t, tc.expectedDeviceIdx, result)
		})
	}
}
