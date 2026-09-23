# Application logs

SEMOSS can capture project-scoped application logs for project owners to view
in the live Console or search from the Logs page.

The per-project file captures loggers owned by project code, including custom
reactors, plus messages emitted through the `LogMessage` Pixel reactor.
SEMOSS `prerna.*` framework logs and `EngineLogger` telemetry are excluded so
platform lifecycle activity and log searches do not flood the application log.

Configure the feature in `RDF_Map.prop`:

```properties
# Operational kill switch. Defaults to true when omitted.
APP_LOGGING_ENABLED=true

# Optional deployment overrides.
APP_LOG_DIRECTORY=/var/log/semoss/apps
APP_LOG_MAX_FILE_SIZE=10MB
APP_LOG_MAX_FILES=5
```

Set `APP_LOGGING_ENABLED=false` and restart SEMOSS to disable appender creation,
historical search, and live log watches. A restart is required because existing
Log4j appenders remain active until the process is restarted.

By default, logs are stored under
`${catalina.base}/logs/apps/<project-id>/app.log`, alongside the server's other
runtime logs. Non-Tomcat processes fall back to `<SEMOSS base>/logs/apps`.
The default rotation retains the active 10 MB file and five rotated files, for
approximately 60 MB per project. Deployments should place this directory on an
appropriately sized volume and monitor aggregate usage across all projects.
