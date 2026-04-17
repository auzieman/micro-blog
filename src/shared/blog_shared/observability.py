import json
import logging
import os
import sys
from contextlib import contextmanager
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_LOGGING_CONFIGURED = False
_REQUESTS_INSTRUMENTED = False


class JsonFormatter(logging.Formatter):
    _skip = {
        "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
        "levelname", "levelno", "lineno", "module", "msecs", "message", "msg",
        "name", "pathname", "process", "processName", "relativeCreated", "stack_info",
        "thread", "threadName", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in self._skip and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True)


def configure_logging() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    _LOGGING_CONFIGURED = True


class BlogTelemetry:
    def __init__(self, service_name: str):
        configure_logging()
        self.service_name = service_name
        self.service_namespace = os.getenv("SERVICE_NAMESPACE", "microblog")
        self.environment = os.getenv("OTEL_ENVIRONMENT", "dev")
        otlp = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/")
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": self.service_namespace,
                "deployment.environment": self.environment,
            }
        )

        trace_provider = TracerProvider(resource=resource)
        trace_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{otlp}/v1/traces")))
        trace.set_tracer_provider(trace_provider)
        self.tracer = trace.get_tracer(service_name)

        metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{otlp}/v1/metrics"))
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
        meter = metrics.get_meter(service_name, "1.0.0")

        self.api_requests = meter.create_counter("blog.api.requests_total")
        self.api_duration = meter.create_histogram("blog.api.duration_ms", unit="ms")
        self.queue_messages = meter.create_counter("blog.queue.messages_total")
        self.process_duration = meter.create_histogram("blog.process.duration_ms", unit="ms")
        self.publish_messages = meter.create_counter("blog.message.publish_total")
        self.db_ops = meter.create_counter("blog.db.ops_total")
        self.db_duration = meter.create_histogram("blog.db.duration_ms", unit="ms")
        self.cache_ops = meter.create_counter("blog.cache.ops_total")
        self.cache_duration = meter.create_histogram("blog.cache.duration_ms", unit="ms")
        self.errors = meter.create_counter("blog.errors_total")
        self.synthetic_requests = meter.create_counter("blog.synthetic.requests_total")
        self.synthetic_duration = meter.create_histogram("blog.synthetic.duration_ms", unit="ms")
        self.synthetic_errors = meter.create_counter("blog.synthetic.errors_total")

        global _REQUESTS_INSTRUMENTED
        if not _REQUESTS_INSTRUMENTED:
            RequestsInstrumentor().instrument()
            _REQUESTS_INSTRUMENTED = True

    def api(self, route: str, method: str, result: str, duration_ms: float) -> None:
        tags = {"route": route, "method": method, "result": result}
        self.api_requests.add(1, tags)
        self.api_duration.record(duration_ms, {"route": route, "method": method})

    def queue(self, destination: str, direction: str, message_type: str, result: str, duration_ms: float) -> None:
        tags = {
            "destination": destination,
            "direction": direction,
            "message_type": message_type,
            "result": result,
        }
        self.queue_messages.add(1, tags)
        self.process_duration.record(duration_ms, {"destination": destination, "message_type": message_type})

    def publish(self, destination: str, message_type: str, result: str = "success") -> None:
        self.publish_messages.add(1, {"destination": destination, "message_type": message_type, "result": result})

    def db(self, operation: str, result: str, duration_ms: float) -> None:
        tags = {"operation": operation, "result": result}
        self.db_ops.add(1, tags)
        self.db_duration.record(duration_ms, tags)

    def cache(self, cache_name: str, operation: str, result: str, duration_ms: float) -> None:
        tags = {"cache": cache_name, "operation": operation, "result": result}
        self.cache_ops.add(1, tags)
        self.cache_duration.record(duration_ms, tags)

    def error(self, source: str, error_type: str) -> None:
        self.errors.add(1, {"source": source, "error_type": error_type})

    def synthetic(self, operation: str, outcome: str, duration_ms: float, error_type: str | None = None) -> None:
        attrs = {"operation": operation, "outcome": outcome}
        self.synthetic_requests.add(1, attrs)
        self.synthetic_duration.record(duration_ms, attrs)
        if error_type:
            self.synthetic_errors.add(1, {"operation": operation, "error_type": error_type})


@contextmanager
def event_scope(logger: logging.Logger, event_name: str, **fields: Any):
    adapter = logging.LoggerAdapter(logger, {"event.domain": "microblog", "event.name": event_name, **fields})
    yield adapter

