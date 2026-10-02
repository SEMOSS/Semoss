# Application logs

SEMOSS can capture project-scoped application logs for project owners to view
in the live Console or search from the Logs page.

The per-project file captures loggers owned by project code, including custom
reactors, plus messages emitted through the `LogMessage` Pixel reactor.
SEMOSS `prerna.*` framework logs and `EngineLogger` telemetry are excluded so
platform lifecycle activity and log searches do not flood the application log.
Each formatted log event is capped at 65,536 characters so one oversized
message cannot bypass the normal file-size bound by an arbitrary amount.

Configure the feature in `RDF_Map.prop`:

```properties
# Explicit opt-in. Missing, blank, invalid, and false values disable logging.
APP_LOGGING_ENABLED=false

# Optional deployment overrides.
APP_LOG_DIRECTORY=/var/log/semoss/apps
APP_LOG_MAX_FILE_SIZE=10MB
APP_LOG_MAX_FILES=5
```

Set `APP_LOGGING_ENABLED=true` and restart SEMOSS to enable appender creation,
historical search, and live log watches. When disabled, the appender's event
filter also rejects writes if an appender remains registered from an earlier
enabled state.

By default, logs are stored under
`${catalina.base}/logs/apps/<project-id>/app.log`, alongside the server's other
runtime logs. Non-Tomcat processes fall back to `<SEMOSS base>/logs/apps`.
Archives use dated names such as `app.log.2026-09-24.1`. Rotation occurs when
the active file reaches its configured size and at the next log event after
midnight. The default retention keeps the active 10 MB file and the five newest
archives across all dates, for approximately 60 MB per project. Deployments
should place this directory on an appropriately sized volume, set pod or
container storage quotas, and monitor aggregate usage across all projects.

This limit applies only to the project application-log feature. Other platform
or worker logs have independent retention policies and are not searched or
streamed by the Application Logs UI.
