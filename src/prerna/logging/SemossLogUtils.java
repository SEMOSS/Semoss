package prerna.logging;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.LocationAware;

import prerna.util.Utility;

public class SemossLogUtils {

	public static final String LOG_ID = "logId";
	public static final String REQUEST_ID = "requestId";

	public static final String USER_ID = "userId";
	public static final String SESSION_ID = "sessionId";
	public static final String CLIENT_IP = "clientIP";

	public static final String SERVICE_NAME = "serviceName";
	public static final String METHOD = "method";
	public static final String ENDPOINT = "endpoint";
	public static final String HOST = "host";
	public static final String REQUEST_TIMESTAMP = "requestTime";
	public static final String LOG_TIMESTAMP = "logTimestamp";

	public static final String IS_SUCCESS = "isSuccess";
	public static final String ENGINE_ID = "engineId";
	public static final String ENGINE_NAME = "engineName";
	public static final String ENGINE_TYPE = "engineType";
	public static final String PROJECT_ID = "projectId";
	public static final String PROJECT_NAME = "projectName";
	public static final String INSIGHT_ID = "insightId";
	public static final String ROOM_ID = "roomId";

	public static final String MESSAGE_ID = "messageId";
	public static final String MESSAGE_TYPE = "messageType";
	public static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	public static final String NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";
	public static final String REQUEST = "request";
	public static final String RESPONSE = "response";
	public static final String METHOD_NAME = "methodName";
	public static final String TIMESTAMP = "timestamp";
	public static final String LEVEL = "logLevel";
	public static final String MESSAGE = "logMessage";
	public static final String REACTOR_SPAN_ID = "reactorSpanId";
	public static final String REACTOR_NAME = "reactorName";
	public static final String SPAN_ID = "spanId";
	public static final String INPUT_REACTOR_NAME = "inputReactorName";
	public static final String OUTPUT_REACTOR_NAME = "outputReactorName";
	public static final String METHOD_ID = "methodId";
	public static final String METHOD_TYPE = "methodType";
	public static final String REQUEST_START_TIME = "requestStartTime";
	public static final String RESPONSE_END_TIME = "responseEndTime";

	public static final String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "KAFKA_BOOTSTRAP_SERVERS_CONFIG";

	/**
	 * Get the engine level logger
	 * 
	 * @return
	 */
	public static Logger getEngineLevelLogger() {
		return LogManager.getLogger("EngineLogger");
	}

	/**
	 * Safely extracts source information from a LogEvent and appends it to a
	 * StringBuilder. Handles cases where includeLocation="false" is set on the
	 * appender.
	 * 
	 * @param event  The LogEvent to extract source information from
	 * @param buffer
	 * @return
	 */
	public static String appendSourceInfo(LogEvent event) {
		StringBuilder builder = new StringBuilder();
		try {
			// Check if the event implements LocationAware and has location info
			if (event instanceof LocationAware) {
				LocationAware locationAware = (LocationAware) event;
				if (!locationAware.requiresLocation()) {
					builder.append("location:unavailable");
					return builder.toString();
				}
			}

			// Attempt to get the source location
			StackTraceElement source = event.getSource();

			if (source != null) {
				// Extract and append source information
				String className = source.getClassName();
				String methodName = source.getMethodName();
				int lineNumber = source.getLineNumber();

				// Append class name (simple name only)
				if (className != null) {
					builder.append(className);
				} else {
					builder.append("unknown");
				}

				// Append method name
				if (methodName != null) {
					builder.append(".").append(methodName);
				} else {
					builder.append(".unknown");
				}

				if (lineNumber > 0) {
					builder.append(":" + lineNumber);
				} else {
					builder.append(":unknown");
				}
			} else {
				// Source is null - location information not available
				builder.append("location:unavailable");
			}

		} catch (UnsupportedOperationException e) {
			// This can happen when location is disabled
			builder.append("location:disabled");
		} catch (Exception e) {
			// Handle any other unexpected exceptions
			builder.append("location:error - ").append(e.getMessage());
		}

		return builder.toString();
	}

	/**
	 * 
	 * @return
	 */
	public static boolean isKafkaUp() {
		String bootStrapServers = Utility.getDIHelperProperty(KAFKA_BOOTSTRAP_SERVERS_CONFIG);
		try (AdminClient adminClient = AdminClient.create(kafkaProperties(bootStrapServers))) {
			adminClient.listTopics(new ListTopicsOptions().timeoutMs(2000)).names().get();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 
	 * @param bootStrapServers
	 * @return
	 */
	public static Properties kafkaProperties(String bootStrapServers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootStrapServers);
		return props;
	}
}
