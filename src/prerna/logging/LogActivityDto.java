package prerna.logging;

import java.sql.Timestamp;

public class LogActivityDto {

	private java.sql.Timestamp startTime;
	private java.sql.Timestamp endTime;
	private java.sql.Timestamp logTimestamp;

	private String request;
	private String response;
	private int tokens;

	private long latency;
	private boolean status;
	private String engineName;
	private String engineType;
	private String userId;
	private String sessionId;
	private String spanId;

	public LogActivityDto() {

	}

	public LogActivityDto(java.sql.Timestamp startTime, java.sql.Timestamp endTime, String request, String response,
			int tokens, long latency, boolean status, String engineName, String engineType, String userId,
			String sessionId, String spanId, java.sql.Timestamp logTimestamp) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.request = request;
		this.response = response;
		this.tokens = tokens;
		this.latency = latency;
		this.status = status;
		this.engineName = engineName;
		this.engineType = engineType;
		this.userId = userId;
		this.sessionId = sessionId;
		this.spanId = spanId;
		this.logTimestamp = logTimestamp;
	}

	public Timestamp getStartTime() {
		return startTime;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getResponse() {
		return response;
	}

	public void setResponse(String response) {
		this.response = response;
	}

	public int getTokens() {
		return tokens;
	}

	public void setTokens(int tokens) {
		this.tokens = tokens;
	}

	public long getLatency() {
		return latency;
	}

	public void setLatency(long latency) {
		this.latency = latency;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public String getEngineName() {
		return engineName;
	}

	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	public String getEngineType() {
		return engineType;
	}

	public void setEngineType(String engineType) {
		this.engineType = engineType;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getSpanId() {
		return spanId;
	}

	public void setSpanId(String spanId) {
		this.spanId = spanId;
	}

	public java.sql.Timestamp getLogTimestamp() {
		return logTimestamp;
	}

	public void setLogTimestamp(java.sql.Timestamp logTimestamp) {
		this.logTimestamp = logTimestamp;
	}

}
