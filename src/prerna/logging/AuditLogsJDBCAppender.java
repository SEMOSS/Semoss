package prerna.logging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
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

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

@Plugin(name = "AuditLogsJDBCAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class AuditLogsJDBCAppender extends AbstractAppender {

	private final String insertSQL;
	private final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	protected AuditLogsJDBCAppender(String name, Filter filter, Layout<String> layout) {
		super(name, filter, layout, true, null);

		// SQL for inserting into audit_logs table
		this.insertSQL = """
				INSERT INTO audit_logs (
				    is_success, engine_id, engine_name, engine_type, input_reactor_name,
				    insight_id, log_id, log_level, log_timestamp, logger_name,
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
			var contextData = event.getContextData();

			// Generate unique log_id if not provided
			String logId = contextData.getValue("logId");
			if (logId == null || logId.isEmpty()) {
				logId = UUID.randomUUID().toString();
			}

			// Map all fields to the audit_logs table columns
			stmt.setBoolean(1, getBooleanValue(contextData.getValue("isSuccess"), true)); // is_success
			stmt.setString(2, contextData.getValue("engineId")); // engine_id
			stmt.setString(3, contextData.getValue("engineName")); // engine_name
			stmt.setString(4, contextData.getValue("engineType")); // engine_type
			stmt.setString(5, contextData.getValue("inputReactorName")); // input_reactor_name
			stmt.setString(6, contextData.getValue("insightId")); // insight_id
			stmt.setString(7, logId); // log_id (PRIMARY KEY)
			stmt.setString(8, event.getLevel().toString()); // log_level
			stmt.setTimestamp(9, new Timestamp(event.getTimeMillis())); // log_timestamp
			stmt.setString(10, event.getLoggerName()); // logger_name
			stmt.setString(11, contextData.getValue("methodId")); // method_id
			stmt.setString(12, contextData.getValue("methodName")); // method_name
			stmt.setString(13, contextData.getValue("methodType")); // method_type
			stmt.setString(14, contextData.getValue("numberOfTokensInPrompt")); // number_of_tokens_in_prompt
			stmt.setString(15, contextData.getValue("numberOfTokensInResponse")); // number_of_tokens_in_response
			stmt.setString(16, contextData.getValue("outputReactorName")); // output_reactor_name
			stmt.setString(17, contextData.getValue("projectId")); // project_id
			stmt.setString(18, contextData.getValue("projectName")); // project_name
			stmt.setString(19, contextData.getValue("requestStartTime")); // request_start_time
			stmt.setString(20, contextData.getValue("responseEndTime")); // response_end_time
			stmt.setString(21, contextData.getValue("roomId")); // room_id
			stmt.setString(22, contextData.getValue("sessionId")); // session_id
			stmt.setString(23, contextData.getValue("spanId")); // span_id
			stmt.setString(24, contextData.getValue("userId")); // user_id
			stmt.setString(25, event.getMessage().getFormattedMessage()); // message
			stmt.setString(26, contextData.getValue("request")); // request (longtext)
			stmt.setString(27, contextData.getValue("response")); // response (longtext)

			stmt.executeUpdate();
		} catch (SQLException e) {
			LOGGER.error("Failed to insert audit log into database", e);
		} finally {
			ConnectionUtils.closeAllDbConnectionsIfPooling(auditLogs, stmt);
		}
	}

	/**
	 * Helper method to convert string to boolean, with default value
	 */
	private boolean getBooleanValue(String value, boolean defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		}
		return "true".equalsIgnoreCase(value) || "1".equals(value) || "success".equalsIgnoreCase(value);
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
