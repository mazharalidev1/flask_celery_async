## [0.4.0] - 2026-10-07

### Added

- Added Django integration through `DjangoAsyncCelery`
- Added framework-neutral `AsyncCeleryCore` for shared Celery configuration
- Added Django async task execution support
- Added Django ORM compatibility for async tasks
- Added task-level monitoring metrics
- Added per-task received, started, completed, failed, and cancelled counters
- Added per-task average runtime tracking
- Added per-task average queue wait-time tracking
- Added worker and task labels to Prometheus metrics
- Added configurable task monitoring and metrics support
- Added expanded Grafana dashboard with worker-level and task-level panels
- Added Prometheus metrics for task-level execution statistics

### Improved

- Improved framework separation between the core executor and Flask integration
- Improved monitoring statistics across workers and individual task types
- Improved task metrics persistence independently of bounded task history
- Improved configuration handling through the shared async Celery core
- Improved Django worker integration and task registration
- Improved documentation for Flask and Django usage
- Improved project structure to support multiple web frameworks

### Documentation

- Updated README.md for the new Django integration
- Added Django async task and ORM examples
- Added task-level monitoring and Prometheus metrics documentation
- Updated Grafana dashboard documentation
- Updated architecture, project structure, and public API documentation

## [0.5.0] - 2026-10-14

### Added
- Optional OpenTelemetry integration through the `otel` extra.
- Distributed trace-context propagation through Celery task headers.
- Task spans with task identity, worker, and outcome attributes.
- Execution-duration and executor-wait span attributes.
- Retry and cancellation outcome recording.
- Integration tests for real-worker tracing and retry context propagation.

### Changed
- Async task tracing records retries separately from ordinary failures.

### Compatibility
- OpenTelemetry remains optional; the core package can run without it.