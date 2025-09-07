package prerna.logging;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

public class SemossLogUtils {

	public static final String LOG_USER_ID = "userId";
	public static final String LOG_SESSION_ID = "sessionId";
	public static final String LOG_CLIENT_IP = "clientIP";
	public static final String LOG_REQUEST_ID = "requestId";
	public static final String LOG_SERVICE_NAME = "serviceName";
	public static final String LOG_METHOD = "method";
	public static final String LOG_ENDPOINT = "endpoint";
	public static final String LOG_HOST = "host";
	public static final String REQUEST_TIMESTAMP = "requestTime";
	public static final String LOG_TIMESTAMP = "logTimestamp";

	public static final String AUDIT_LOG_INSIGHT_ID = "insightId";
	public static final String AUDIT_LOG_SESSION_ID = "sessionId";
	public static final String AUDIT_LOG_ENGINE_ID = "engineId";
	public static final String AUDIT_LOG_ENGINE_NAME = "engineName";
	public static final String AUDIT_LOG_ENGINE_TYPE = "engineType";
	public static final String AUDIT_LOG_USER_ID = "userId";
	public static final String AUDIT_LOG_PROJECT_ID = "projectId";
	public static final String AUDIT_LOG_PROJECT_NAME = "projectName";
	public static final String AUDIT_LOG_ROOM_ID = "roomId";
	public static final String AUDIT_LOG_MESSAGE_ID = "messageId";
	public static final String AUDIT_LOG_MESSAGE_TYPE = "messageType";
	public static final String AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	public static final String AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";
	public static final String AUDIT_LOG_REQUEST = "request";
	public static final String AUDIT_LOG_RESPONSE = "response";
	public static final String AUDIT_LOG_METHOD_NAME = "methodName";
	public static final String AUDIT_LOG_TIMESTAMP = "timestamp";
	public static final String AUDIT_LOG_LEVEL = "logLevel";
	public static final String AUDIT_LOG_MESSAGE = "logMessage";
	public static final String AUDIT_LOG_REACTOR_SPAN_ID = "reactorSpanId";
	public static final String AUDIT_LOG_REACTOR_NAME = "reactorName";
	public static final String AUDIT_LOG_METHOD_SPAN_ID = "methodSpanId";
	public static final String AUDIT_LOG_INPUT_REACTOR_NAME = "inputReactorName";
	public static final String AUDIT_LOG_OUTPUT_REACTOR_NAME = "outputReactorName";
	public static final String AUDIT_LOG_IS_SUCCESS = "isSuccess";

	public static final String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "KAFKA_BOOTSTRAP_SERVERS_CONFIG";
	public static final String KAFKA_ACKS_CONFIG = "KAFKA_ACKS_CONFIG";
	public static final String KAFKA_RETRIES_CONFIG = "KAFKA_RETRIES_CONFIG";
	public static final String KAFKA_KEY_SERIALIZER_CLASS_CONFIG = "KAFKA_KEY_SERIALIZER_CLASS_CONFIG";
	public static final String KAFKA_VALUE_SERIALIZER_CLASS_CONFIG = "KAFKA_VALUE_SERIALIZER_CLASS_CONFIG";
	public static final String KAFKA_AUDIT_LOG_TOPIC = "KAFKA_AUDIT_LOG_TOPIC";

	/**
	 * Get the engine level logger
	 * 
	 * @return
	 */
	public static Logger getEngineLevelLogger() {
		return LogManager.getLogger("EngineLogger");
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
