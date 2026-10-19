// Package otlp builds the tracer provider of the process, which exports its spans over OTLP/HTTP.
package otlp

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

// Provider is the tracer provider of the process and the exporter behind it.
type Provider struct {
	sdk    *sdktrace.TracerProvider
	tracer tracing.Tracer
}

// New returns the Provider cfg describes; without an endpoint it is off and starts no goroutine.
func New(cfg config.TracingConfig, instanceID string, log *slog.Logger) (*Provider, error) {
	if !cfg.Enabled() {
		return &Provider{}, nil
	}
	exporter, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(cfg.TracesURL()))
	if err != nil {
		return nil, fmt.Errorf("otlp: create the exporter: %w", err)
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("tracing failed", slog.String("error", err.Error()))
	}))
	sdk := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceInstanceID(instanceID),
		)),
	)
	return &Provider{sdk: sdk, tracer: tracing.NewTracer(sdk)}, nil
}

// Tracer returns the Tracer of the process, off when the provider is.
func (p *Provider) Tracer() tracing.Tracer { return p.tracer }

// Shutdown exports the spans still queued, within ctx, and stops the exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.sdk == nil {
		return nil
	}
	if err := p.sdk.Shutdown(ctx); err != nil {
		return fmt.Errorf("otlp: shut down: %w", err)
	}
	return nil
}
