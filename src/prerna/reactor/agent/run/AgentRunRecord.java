/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.reactor.agent.run;

/**
 * One {@code AGENT_RUN} row, read back into memory.
 *
 * <p>
 * Holds the run's identity and status alongside the {@link AgentRunRequest}
 * rehydrated from the {@code REQUEST_JSON} column, which is what lets the
 * worker execute a run it did not receive itself.
 *
 * @param runId   durable id of the run
 * @param roomId  room the run belongs to
 * @param status  status as of the read
 * @param request the submission this run was created from
 * @param userId  owner of the run
 * @param jobId   id the streaming and logging layers key on
 */
public record AgentRunRecord(String runId, String roomId, AgentRunStatus status, AgentRunRequest request, String userId,
		String jobId) {
}
