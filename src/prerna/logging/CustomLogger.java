package prerna.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomLogger extends ExtendedLoggerWrapper {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper mapper = new ObjectMapper();

	protected CustomLogger(Logger logger) {
		super((ExtendedLogger) logger, logger.getName(), logger.getMessageFactory());
	}

	public static CustomLogger getLogger(Class<?> clazz) {
		Logger logger = LogManager.getLogger(clazz);
		return new CustomLogger(logger);
	}

	public void info(MapMessage<?, ?> mapMessage) {
		log(Level.INFO, mapMessage);
	}

	public void debug(MapMessage<?, ?> mapMessage) {
		mapMessage.put(SemossLogUtils.AUDIT_LOG_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
		log(Level.DEBUG, mapMessage);
	}

	public void error(MapMessage<?, ?> mapMessage) {
		mapMessage.put(SemossLogUtils.AUDIT_LOG_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
		log(Level.ERROR, mapMessage);
	}

	public void log(String logLevel, String message, MapMessage<?, ?> mapMessage) {
		log(org.apache.logging.log4j.Level.toLevel(logLevel), message, mapMessage);
	}

	public void debug(String logLevel, String message, MapMessage<?, ?> mapMessage) {
		log(org.apache.logging.log4j.Level.toLevel(logLevel), message, mapMessage);
	}

	public void error(String logLevel, String message, MapMessage<?, ?> mapMessage) {
		log(org.apache.logging.log4j.Level.toLevel(logLevel), message, mapMessage);
	}

}
