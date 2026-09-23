package prerna.engine.impl.model.message;

import java.util.Map;

import com.google.gson.annotations.SerializedName;

/** Durable agent-run attribution carried directly by a room message. */
public final class AgentRunMessageContext {

	@SerializedName("runId")
	private String runId;

	@SerializedName("role")
	private String role;

	@SerializedName("originatingRunId")
	private String originatingRunId;

	@SerializedName("childRunId")
	private String childRunId;

	@SerializedName("completionMode")
	private String completionMode;

	@SerializedName("childStatus")
	private String childStatus;

	private AgentRunMessageContext() {
		// Gson
	}

	public AgentRunMessageContext(String runId, String role) {
		setRunId(runId);
		this.role = trimToNull(role);
	}

	static AgentRunMessageContext fromLegacyOrnaments(Map<String, Object> ornaments) {
		if (ornaments == null) {
			return null;
		}
		String runId = trimToNull(ornaments.get("agentRunId"));
		if (runId == null) {
			return null;
		}
		AgentRunMessageContext context = new AgentRunMessageContext(runId,
				trimToNull(ornaments.get("agentRunRole")));
		context.originatingRunId = trimToNull(ornaments.get("originatingAgentRunId"));
		context.childRunId = trimToNull(ornaments.get("childRunId"));
		context.completionMode = trimToNull(ornaments.get("completionMode"));
		context.childStatus = trimToNull(ornaments.get("childStatus"));
		return context;
	}

	void validate() {
		if (trimToNull(runId) == null) {
			throw new IllegalStateException("agentRun.runId is required");
		}
	}

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		String normalized = trimToNull(runId);
		if (normalized == null) {
			throw new IllegalArgumentException("runId is required");
		}
		this.runId = normalized;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = trimToNull(role);
	}

	public String getOriginatingRunId() {
		return originatingRunId;
	}

	public void setOriginatingRunId(String originatingRunId) {
		this.originatingRunId = trimToNull(originatingRunId);
	}

	public String getChildRunId() {
		return childRunId;
	}

	public void setChildRunId(String childRunId) {
		this.childRunId = trimToNull(childRunId);
	}

	public String getCompletionMode() {
		return completionMode;
	}

	public void setCompletionMode(String completionMode) {
		this.completionMode = trimToNull(completionMode);
	}

	public String getChildStatus() {
		return childStatus;
	}

	public void setChildStatus(String childStatus) {
		this.childStatus = trimToNull(childStatus);
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}
}
