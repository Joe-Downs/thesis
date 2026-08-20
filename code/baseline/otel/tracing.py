"""Shared OpenTelemetry tracing setup for benchmark scripts.

Writes spans to a local JSONL file instead of exporting over the network, so
benchmark scripts running on cluster nodes never need connectivity back to
wherever the telemetry is analyzed (see baseline/otel/ingest.py). Each line
in the output file is one span's `ReadableSpan.to_json()` representation.
"""
import collections.abc
import datetime
import time
from pathlib import Path

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)


class FileSpanExporter(SpanExporter):
    """Appends each span as one JSON line to a file.

    Used with a SimpleSpanProcessor (synchronous export on every span end)
    rather than the usual BatchSpanProcessor: a benchmark.export span can
    then record the actual measured write duration on the run's own span
    tree. That's not possible with batched/async export, since a span can't
    wrap the eventual export of itself. Synchronous export also means no
    buffered spans are lost if a sweep is interrupted mid-run.
    """

    def __init__(self, path: Path):
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self.last_duration_s = 0.0

    def export(self, spans: collections.abc.Sequence[ReadableSpan]) -> SpanExportResult:
        start = time.monotonic()
        with self._path.open("a") as f:
            for span in spans:
                f.write(span.to_json(indent=None))
                f.write("\n")
        self.last_duration_s = time.monotonic() - start
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def trace_file_path(traces_dir: Path, experiment: str) -> Path:
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return traces_dir / f"{experiment}_{timestamp}.jsonl"


def get_tracer(service_name: str, output_path: Path) -> tuple[trace.Tracer, FileSpanExporter]:
    """Configure a TracerProvider that writes spans to `output_path` and
    return (Tracer, exporter) for `service_name`. The exporter is returned so
    callers can read `exporter.last_duration_s` after a span ends, to record
    actual export overhead as an attribute (see baseline/bench.py). Call once
    per script invocation."""
    exporter = FileSpanExporter(output_path)
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name), exporter


def shutdown_tracing() -> None:
    """Shut down the current TracerProvider. Call before exit."""
    provider = trace.get_tracer_provider()
    if hasattr(provider, "shutdown"):
        provider.shutdown()
