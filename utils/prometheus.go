package utils

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	PrometheusSyncStarted         *prometheus.CounterVec
	PrometheusSyncFinished        *prometheus.CounterVec
	PrometheusBundlesSynced       *prometheus.CounterVec
	PrometheusSyncStepFailedRetry *prometheus.CounterVec

	PrometheusCurrentBundleHeight *prometheus.GaugeVec
	PrometheusLastSyncDuration    *prometheus.GaugeVec
)

func StartPrometheus(port string) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(":"+port, nil)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("prometheus start error")
		}
	}()
	logger.Info().Str("port", port).Msg("Started prometheus")
}

func init() {
	var labelNames = []string{"connection"}

	PrometheusSyncStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_started",
	}, labelNames)

	PrometheusSyncFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_finished",
	}, labelNames)

	PrometheusBundlesSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bundles_synced",
	}, labelNames)

	PrometheusSyncStepFailedRetry = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_step_failed_retry",
	}, labelNames)

	PrometheusCurrentBundleHeight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "current_bundle_height",
	}, labelNames)

	PrometheusLastSyncDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "last_sync_duration",
	}, labelNames)
}
