package prerna.logging;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import prerna.util.Utility;

@Plugin(name = "QueueAppender", category = "Core", elementType = "appender", printObject = true)
public class QueueAppender extends AbstractAppender {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private String destinationType;
	private String topicOrQueue;
	private String bootstrapServers;

	private KafkaProducer<String, String> kafkaProducer;
	private Connection rabbitConnection;
	private Channel rabbitChannel;

	protected QueueAppender(String name, String destinationType, String topicOrQueue, String bootstrapServers,
			Layout<? extends Serializable> layout, Filter filter) {
		super(name, filter, layout, true);

		this.destinationType = destinationType;
		this.topicOrQueue = topicOrQueue;
		this.bootstrapServers = bootstrapServers;
		/*
		 * if ("kafka".equalsIgnoreCase(destinationType) &&
		 * !isKafkaUp(bootstrapServers)) {
		 * System.out.println("Kafka not avaliable. QueueAppender not initialized.");
		 * return; }
		 */
	}

	@PluginFactory
	public static QueueAppender createAppender(@PluginAttribute("name") String name,
			@PluginAttribute("destinationType") String destinationType,
			@PluginAttribute("topicOrQueue") String topicOrQueue,
			@PluginAttribute("bootstrapServers") String bootstrapServers,
			@PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") Filter filter) {

		if (name == null) {
			LOGGER.error("No name provided for QueueAppender");
			return null;
		}
		if (layout == null) {
			layout = PatternLayout.createDefaultLayout();
		}
		return new QueueAppender(name, destinationType, topicOrQueue, bootstrapServers, layout, filter);
	}

	@Override
	public void append(LogEvent event) {
		String message = new String(getLayout().toByteArray(event));
		try {
			if ("kafka".equalsIgnoreCase(destinationType) && Utility.isKafkaUp()) {

				try {
					if (event.getLoggerName() != null && event.getLoggerName().startsWith("org.apache.kafka")) {
						return;
					}
					if ("kafka".equalsIgnoreCase(destinationType)) {
						Properties props = new Properties();
						props.put("bootstrap.servers", bootstrapServers);
						props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
						props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
						kafkaProducer = new KafkaProducer<>(props);
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

					kafkaProducer.send(new ProducerRecord<>(topicOrQueue, dateTimeStr, value));

				} catch (Exception e) {

				}

			} else if ("rabbitmq".equalsIgnoreCase(destinationType)) {
				// Lazy init RabbitMQ connection/channel
				if (rabbitConnection == null || rabbitChannel == null) {
					try {
						ConnectionFactory factory = new ConnectionFactory();
						factory.setHost(bootstrapServers);
						rabbitConnection = factory.newConnection();
						rabbitChannel = rabbitConnection.createChannel();
						rabbitChannel.queueDeclare(topicOrQueue, true, false, false, null);
					} catch (Exception e) {
						System.err.println("RabbitMQ not available. Skipping log: " + e.getMessage());
						return; // skip sending this log
					}
				}
				rabbitChannel.basicPublish("", topicOrQueue, null, message.getBytes());
			}
		} catch (Exception e) {
			System.err.println("error while"+e.getMessage());
		}
	}

	@Override
	public void stop() {
		super.stop();
		try {
			if (kafkaProducer != null)
				kafkaProducer.close();
			if (rabbitChannel != null)
				rabbitChannel.close();
			if (rabbitConnection != null)
				rabbitConnection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/*
	 * private final IQueueLogger queueLogger;
	 * 
	 * private final ObjectMapper objectMapper = new ObjectMapper();
	 * 
	 * protected QueueAppender(String name, Filter filter, Layout<? extends
	 * Serializable> layout, boolean ignoreExceptions, IQueueLogger queueLogger) {
	 * super(name, filter, layout, ignoreExceptions); this.queueLogger =
	 * queueLogger; }
	 * 
	 * @PluginFactory public static QueueAppender
	 * createAppender(@PluginAttribute("name") String name,
	 * 
	 * @PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<?
	 * extends Serializable> layout,
	 * 
	 * @PluginAttribute("loggerClass") String
	 * loggerClass, @PluginAttribute("loggerConfig") String loggerConfig) {
	 * 
	 * if (layout == null) { layout = PatternLayout.createDefaultLayout(); }
	 * IQueueLogger iQueueLogger = null; try { Class<?> clazz =
	 * Class.forName(loggerClass); iQueueLogger = (IQueueLogger)
	 * clazz.getConstructor(String.class).newInstance(loggerConfig); } catch
	 * (Exception e) {
	 * 
	 * }
	 * 
	 * return new QueueAppender(name, filter, layout, false, iQueueLogger);
	 * 
	 * }
	 * 
	 * @Override public void start() { super.start(); queueLogger.init(); }
	 * 
	 * @Override public void append(LogEvent event) { try { if(event.getLoggerName()
	 * !=null && event.getLoggerName().startsWith("org.apache.kafka")) { return; }
	 * AuditLogEvent auditLogEvent = new AuditLogEvent();
	 * 
	 * auditLogEvent.setLevel(event.getLevel().name());
	 * auditLogEvent.setLogger(event.getLoggerName());
	 * auditLogEvent.setThread(event.getThreadName());
	 * auditLogEvent.setMdc(event.getContextMap()); if (event.getMessage()
	 * instanceof MapMessage) { MapMessage<?, ?> mapMesssage = (MapMessage<?, ?>)
	 * event.getMessage(); auditLogEvent.setCustomKeyValueMap((Map<String, String>)
	 * mapMesssage.getData()); } else {
	 * auditLogEvent.setMessage(event.getMessage().getFormattedMessage()); }
	 * 
	 * LocalDateTime dateTime =
	 * Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
	 * .toLocalDateTime(); String dateTimeStr = dateTime.toString();
	 * auditLogEvent.setTimestamp(dateTimeStr); String value =
	 * objectMapper.writeValueAsString(auditLogEvent);
	 * 
	 * queueLogger.send(dateTimeStr, value);
	 * 
	 * } catch (Exception e) {
	 * 
	 * }
	 * 
	 * }
	 */
}
