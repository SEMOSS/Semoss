package prerna.logging;

import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.util.ReadOnlyStringMap;

public class AuditLogEvent {

	private String timestamp;
	private String level;
	private String logger;
	private String message;
	private String thread;
	private ReadOnlyStringMap mdc;
	private Map<String, String> customKeyValueMap;

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getThread() {
		return thread;
	}

	public void setThread(String thread) {
		this.thread = thread;
	}

	public ReadOnlyStringMap getMdc() {
		return mdc;
	}

	public void setMdc(ReadOnlyStringMap readOnlyStringMap) {
		this.mdc = readOnlyStringMap;
	}

	public Map<String, String> getCustomKeyValueMap() {
		return customKeyValueMap;
	}

	public void setCustomKeyValueMap(Map<String, String> customKeyValueMap) {
		this.customKeyValueMap = customKeyValueMap;
	}

	@Override
	public int hashCode() {
		return Objects.hash(customKeyValueMap, level, logger, message, thread, mdc, timestamp);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		AuditLogEvent other = (AuditLogEvent) obj;
		return Objects.equals(customKeyValueMap, other.customKeyValueMap) && Objects.equals(level, other.level)
				&& Objects.equals(logger, other.logger) && Objects.equals(message, other.message)
				&& Objects.equals(thread, other.thread) && Objects.equals(mdc, other.mdc)
				&& timestamp == other.timestamp;
	}

	@Override
	public String toString() {
		return "AuditLogEvent [timestamp=" + timestamp + ", level=" + level + ", logger=" + logger + ", message="
				+ message + ", thread=" + thread + ", mdc=" + mdc + ", customKeyValueMap=" + customKeyValueMap + "]";
	}

}
