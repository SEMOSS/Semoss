/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.reactor.agent.run;

public final class AgentRunRecord {

	private final String runId;
	private final String roomId;
	private final AgentRunStatus status;
	private final RunAgentRequest request;
	private final String userId;
	private final String jobId;

	public AgentRunRecord(String runId, String roomId, AgentRunStatus status, RunAgentRequest request, String userId,
			String jobId) {
		this.runId = runId;
		this.roomId = roomId;
		this.status = status;
		this.request = request;
		this.userId = userId;
		this.jobId = jobId;
	}

	public String getRunId() {
		return runId;
	}

	public String getRoomId() {
		return roomId;
	}

	public AgentRunStatus getStatus() {
		return status;
	}

	public RunAgentRequest getRequest() {
		return request;
	}

	public String getUserId() {
		return userId;
	}

	public String getJobId() {
		return jobId;
	}
}
