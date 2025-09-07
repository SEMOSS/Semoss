
package prerna.logging;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

@Plugin(name = "ServerLogsJDBCAppender", category = "Core", elementType = "appender", printObject = true)
public class ServerLogsJDBCAppender extends AbstractAppender {

	protected ServerLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
	}

	@Override
	public void append(LogEvent event) {
		if (!AuditLogsDbUtils.isInitalized()) {
			return;
		}

		IRDBMSEngine auditLogs = (IRDBMSEngine) Utility.getDatabase(Constants.AUDIT_LOGS_DATABASE_NAME);

		final String insertSql = "INSERT INTO SERVER_LOGS (LEVEL, LOGGER, THREAD_NAME, LOG_TIMESTAMP, MESSAGE) VALUES (?, ?, ?, ?, ?)";
		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			stmt = connection.prepareStatement(insertSql);

			stmt.setString(1, event.getLevel().toString());
			stmt.setString(2, event.getLoggerName());
			stmt.setString(3, event.getThreadName());
			stmt.setTimestamp(4, new Timestamp(event.getTimeMillis()));
			stmt.setString(5, event.getMessage().getFormattedMessage());

			stmt.executeUpdate();
		} catch (SQLException e) {
			LOGGER.error("Failed to insert audit log into database", e);
		} finally {
			ConnectionUtils.closeAllDbConnectionsIfPooling(auditLogs, stmt);
		}
	}

	@PluginFactory
	public static ServerLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions) {

		if (name == null) {
			LOGGER.error("No name provided for DatabaseAppender");
			return null;
		}

		return new ServerLogsJDBCAppender(name, filter, layout, ignoreExceptions);
	}
}
