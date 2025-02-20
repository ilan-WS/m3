// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package downsample

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Downsampler is a downsampler.
type Downsampler interface {
	NewMetricsAppender() (MetricsAppender, error)
	// Enabled indicates whether the downsampler is enabled or not. A
	// downsampler is enabled if there are aggregated ClusterNamespaces
	// that exist as downsampling only applies to aggregations.
	Enabled() bool
}

// MetricsAppender is a metrics appender that can build a samples
// appender, only valid to use with a single caller at a time.
type MetricsAppender interface {
	// NextMetric progresses to building the next metric.
	NextMetric()
	// AddTag adds a tag to the current metric being built.
	AddTag(name, value []byte)
	// SamplesAppender returns a samples appender for the current
	// metric built with the tags that have been set.
	SamplesAppender(opts SampleAppenderOptions) (SamplesAppenderResult, error)
	// Finalize finalizes the entire metrics appender for reuse.
	Finalize()
}

// SamplesAppenderResult is the result from a SamplesAppender call.
type SamplesAppenderResult struct {
	SamplesAppender     SamplesAppender
	IsDropPolicyApplied bool
	ShouldDropTimestamp bool
}

// SampleAppenderOptions defines the options being used when constructing
// the samples appender for a metric.
type SampleAppenderOptions struct {
	Override         bool
	OverrideRules    SamplesAppenderOverrideRules
	SeriesAttributes ts.SeriesAttributes
}

// SamplesAppenderOverrideRules provides override rules to
// use instead of matching against default and dynamic matched rules
// for an ID.
type SamplesAppenderOverrideRules struct {
	MappingRules []AutoMappingRule
}

// SamplesAppender is a downsampling samples appender,
// that can only be called by a single caller at a time.
type SamplesAppender interface {
	AppendUntimedCounterSample(value int64, annotation []byte) error
	AppendUntimedGaugeSample(value float64, annotation []byte) error
	AppendUntimedTimerSample(value float64, annotation []byte) error
	AppendCounterSample(t time.Time, value int64, annotation []byte) error
	AppendGaugeSample(t time.Time, value float64, annotation []byte) error
	AppendTimerSample(t time.Time, value float64, annotation []byte) error
}

type downsampler struct {
	opts DownsamplerOptions
	agg  agg

	sync.RWMutex
	metricsAppenderOpts metricsAppenderOptions
	enabled             bool
}

type downsamplerOptions struct {
	opts DownsamplerOptions
	agg  agg
}

func newDownsampler(opts downsamplerOptions) (*downsampler, error) {
	if err := opts.opts.validate(); err != nil {
		return nil, err
	}

	downsampler := &downsampler{
		opts:                opts.opts,
		agg:                 opts.agg,
		metricsAppenderOpts: defaultMetricsAppenderOptions(opts.opts, opts.agg),
	}

	// No need to retain watch as NamespaceWatcher.Close() will handle closing any watches
	// generated by creating listeners.
	downsampler.opts.ClusterNamespacesWatcher.RegisterListener(downsampler)

	return downsampler, nil
}

func defaultMetricsAppenderOptions(opts DownsamplerOptions, agg agg) metricsAppenderOptions {
	debugLogging := false
	logger := opts.InstrumentOptions.Logger()
	if logger.Check(zapcore.DebugLevel, "debug") != nil {
		debugLogging = true
	}
	scope := opts.InstrumentOptions.MetricsScope().SubScope("metrics_appender")
	metrics := metricsAppenderMetrics{
		processedCountNonRollup: scope.Tagged(map[string]string{"agg_type": "non_rollup"}).Counter("processed"),
		processedCountRollup:    scope.Tagged(map[string]string{"agg_type": "rollup"}).Counter("processed"),
		operationsCount:         scope.Counter("operations_processed"),
	}

	return metricsAppenderOptions{
		agg:                    agg.aggregator,
		clientRemote:           agg.clientRemote,
		clockOpts:              agg.clockOpts,
		tagEncoderPool:         agg.pools.tagEncoderPool,
		matcher:                agg.matcher,
		metricTagsIteratorPool: agg.pools.metricTagsIteratorPool,
		debugLogging:           debugLogging,
		logger:                 logger,
		untimedRollups:         agg.untimedRollups,
		metrics:                metrics,
	}
}

func (d *downsampler) NewMetricsAppender() (MetricsAppender, error) {
	metricsAppender := d.agg.pools.metricsAppenderPool.Get()

	d.RLock()
	newMetricsAppenderOpts := d.metricsAppenderOpts
	d.RUnlock()

	metricsAppender.reset(newMetricsAppenderOpts)

	return metricsAppender, nil
}

func (d *downsampler) Enabled() bool {
	d.RLock()
	defer d.RUnlock()

	return d.enabled
}

func (d *downsampler) OnUpdate(namespaces m3.ClusterNamespaces) {
	logger := d.opts.InstrumentOptions.Logger()

	if len(namespaces) == 0 {
		logger.Debug("received empty list of namespaces. not updating staged metadata")
		return
	}

	var hasAggregatedNamespaces bool
	for _, namespace := range namespaces {
		attrs := namespace.Options().Attributes()
		if attrs.MetricsType == storagemetadata.AggregatedMetricsType {
			hasAggregatedNamespaces = true
			break
		}
	}

	autoMappingRules, err := NewAutoMappingRules(namespaces)
	if err != nil {
		logger.Error("could not generate automapping rules for aggregated namespaces."+
			" aggregations will continue with current configuration.", zap.Error(err))
		return
	}
	defaultStagedMetadatasProtos := make([]metricpb.StagedMetadatas, 0, len(autoMappingRules))
	for _, rule := range autoMappingRules {
		metadatas, err := rule.StagedMetadatas()
		if err != nil {
			logger.Error("could not generate staged metadata from automapping rules."+
				" aggregations will continue with current configuration.", zap.Error(err))
			return
		}

		var metadatasProto metricpb.StagedMetadatas
		if err := metadatas.ToProto(&metadatasProto); err != nil {
			logger.Error("could not generate staged metadata from automapping rules."+
				" aggregations will continue with current configuration.", zap.Error(err))
			return
		}

		defaultStagedMetadatasProtos = append(defaultStagedMetadatasProtos, metadatasProto)
	}

	d.Lock()
	d.metricsAppenderOpts.defaultStagedMetadatasProtos = defaultStagedMetadatasProtos
	// Can only downsample when aggregated namespaces are available.
	d.enabled = hasAggregatedNamespaces
	d.Unlock()
}
