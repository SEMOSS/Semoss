package prerna.logging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

@Plugin(name = "AuditLogsJDBCAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class AuditLogsJDBCAppender extends AbstractAppender {

	private final String insertSQL;

	protected AuditLogsJDBCAppender(String name, Filter filter, Layout<String> layout) {
		super(name, filter, layout, true, null);

		// SQL for inserting into audit_logs table
		this.insertSQL = """
				INSERT INTO audit_logs (
				    log_id, is_success, engine_id, engine_name, engine_type, input_reactor_name,
				    insight_id, log_level, log_timestamp, logger_name,
				    method_id, method_name, method_type, number_of_tokens_in_prompt,
				    number_of_tokens_in_response, output_reactor_name, project_id,
				    project_name, request_start_time, response_end_time, room_id,
				    session_id, span_id, user_id, message, request, response
				) VALUES (
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?, ?
				)
				""";
	}

	@Override
	public void append(LogEvent event) {
		IRDBMSEngine auditLogs = (IRDBMSEngine) Utility.getDatabase(Constants.AUDIT_LOGS_DATABASE_NAME);
		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			stmt = connection.prepareStatement(insertSQL);
			// Get context data for custom fields
			ReadOnlyStringMap contextData = event.getContextData();
			MapMessage<?, ?> message = (MapMessage<?, ?>) event.getMessage();

			// Generate unique log_id if not provided
			String logId = contextData.getValue(SemossLogUtils.LOG_ID);
			if (logId == null || logId.isEmpty()) {
				logId = UUID.randomUUID().toString();
			}

			// Map all fields to the audit_logs table columns
			int parameterIndex = 1;
			stmt.setString(parameterIndex++, logId); // log_id (PRIMARY KEY)
			stmt.setBoolean(parameterIndex++, getBooleanValue(SemossLogUtils.IS_SUCCESS, contextData, message)); // is_success
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.ENGINE_ID, contextData, message)); // engine_id
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.ENGINE_NAME, contextData, message)); // engine_name
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.ENGINE_TYPE, contextData, message)); // engine_type
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.INPUT_REACTOR_NAME, contextData, message)); // input_reactor_name
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.INSIGHT_ID, contextData, message)); // insight_id
			stmt.setString(parameterIndex++, event.getLevel().toString()); // log_level
			stmt.setTimestamp(parameterIndex++, new Timestamp(event.getTimeMillis())); // log_timestamp
			stmt.setString(parameterIndex++, event.getLoggerName()); // logger_name
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.METHOD_ID, contextData, message)); // method_id
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.METHOD_NAME, contextData, message)); // method_name
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.METHOD_TYPE, contextData, message)); // method_type
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT, contextData, message)); // number_of_tokens_in_prompt
			stmt.setString(parameterIndex++,
					getValue(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE, contextData, message)); // number_of_tokens_in_response
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.OUTPUT_REACTOR_NAME, contextData, message)); // output_reactor_name
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.PROJECT_ID, contextData, message)); // project_id
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.PROJECT_NAME, contextData, message)); // project_name
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.REQUEST_START_TIME, contextData, message)); // request_start_time
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.RESPONSE_END_TIME, contextData, message)); // response_end_time
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.ROOM_ID, contextData, message)); // room_id
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.SESSION_ID, contextData, message)); // session_id
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.SPAN_ID, contextData, message)); // span_id
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.USER_ID, contextData, message)); // user_id
			stmt.setString(parameterIndex++, event.getMessage().getFormattedMessage()); // message
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.REQUEST, contextData, message)); // request
			stmt.setString(parameterIndex++, getValue(SemossLogUtils.RESPONSE, contextData, message)); // response

			stmt.executeUpdate();
		} catch (SQLException e) {
			LOGGER.error("Failed to insert audit log into database", e);
		} finally {
			ConnectionUtils.closeAllDbConnectionsIfPooling(auditLogs, stmt);
		}
	}

	/**
	 * Helper method to get a value from the context data or the message
	 * 
	 * @param key
	 * @param contextData
	 * @param message
	 * @return
	 */
	private String getValue(String key, ReadOnlyStringMap contextData, MapMessage<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			return value;
		}
		Object messageValue = message.get(key);
		if (messageValue != null) {
			return messageValue.toString();
		}
		return null;
	}

	/**
	 * Helper method to convert string to boolean, with default value
	 * 
	 * @param value
	 * @param defaultValue
	 * @return
	 */
	private boolean getBooleanValue(String key, ReadOnlyStringMap contextData, MapMessage<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			return Boolean.parseBoolean(value);
		}
		Object messageValue = message.get(key);
		if (messageValue != null) {
			return Boolean.parseBoolean(messageValue.toString());
		}
		return true;
	}

	@PluginFactory
	public static AuditLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<String> layout) {

		if (name == null) {
			LOGGER.error("No name provided for CustomDirectJDBCLogger");
			return null;
		}

		return new AuditLogsJDBCAppender(name, filter, layout);
	}

}
