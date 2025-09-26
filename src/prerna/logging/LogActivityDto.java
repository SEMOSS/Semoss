package prerna.logging;

import java.sql.Timestamp;

import org.json.JSONObject;

public class LogActivityDto extends JSONObject {

	private java.sql.Timestamp startTime;
	private java.sql.Timestamp endTime;
	private String payload;
	private String response;
	private int tokens;

	private long latency;
	private boolean status;
	private String engineName;
	private String engineType;

	public LogActivityDto(java.sql.Timestamp startTime, java.sql.Timestamp endTime, String payload, String response,
			int tokens, long latency, boolean status, String engineName, String engineType) {
		super();
		this.startTime = startTime;
		this.endTime = endTime;
		this.payload = payload;
		this.response = response;
		this.tokens = tokens;
		this.latency = latency;
		this.status = status;
		this.engineName = engineName;
		this.engineType = engineType;
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

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
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

}
