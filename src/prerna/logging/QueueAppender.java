package prerna.logging;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.MapMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.util.Constants;

@Plugin(name = "QueueAppender", category = "Core", elementType = "appender", printObject = true)
public class QueueAppender extends AbstractAppender {

	private final IQueueLogger queueLogger;

	private final ObjectMapper objectMapper = new ObjectMapper();

	protected QueueAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
			IQueueLogger queueLogger) {
		super(name, filter, layout, ignoreExceptions);
		this.queueLogger = queueLogger;
	}

	@PluginFactory
	public static QueueAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginAttribute("loggerClass") String loggerClass, @PluginAttribute("loggerConfig") String loggerConfig) {

		if (layout == null) {
			layout = PatternLayout.createDefaultLayout();
		}
		IQueueLogger iQueueLogger = null;
		try {
			Class<?> clazz = Class.forName(loggerClass);
			iQueueLogger = (IQueueLogger) clazz.getConstructor(String.class).newInstance(loggerConfig);
		} catch (Exception e) {

		}

		return new QueueAppender(name, filter, layout, false, iQueueLogger);

	}

	@Override
	public void start() {
		super.start();
		queueLogger.init();
	}

	@Override
	public void append(LogEvent event) {
		try {
if(event.getLoggerName() !=null && event.getLoggerName().startsWith("org.apache.kafka")) {
	return;
}
			AuditLogEvent auditLogEvent = new AuditLogEvent();
			
			auditLogEvent.setLevel(event.getLevel().name());
			auditLogEvent.setLogger(event.getLoggerName());
			auditLogEvent.setThread(event.getThreadName());
			auditLogEvent.setMdc(event.getContextMap());
			if (event.getMessage() instanceof MapMessage) {
				MapMessage<?, ?> mapMesssage = (MapMessage<?, ?>) event.getMessage();
				auditLogEvent.setCustomKeyValueMap((Map<String, String>) mapMesssage.getData());
			} else {
				auditLogEvent.setMessage(event.getMessage().getFormattedMessage());
			}

			LocalDateTime dateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
					.toLocalDateTime();
			String dateTimeStr = dateTime.toString();
			auditLogEvent.setTimestamp(dateTimeStr);
			String value = objectMapper.writeValueAsString(auditLogEvent);

			queueLogger.send(dateTimeStr, value);

		} catch (Exception e) {

		}

	}

}
