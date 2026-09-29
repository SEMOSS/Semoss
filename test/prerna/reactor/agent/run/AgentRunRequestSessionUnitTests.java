package prerna.reactor.agent.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.om.ThreadStore;

class AgentRunRequestSessionUnitTests {

	@AfterEach
	void cleanThreadStore() {
		ThreadStore.remove();
	}

	@Test
	void immutableRunCopiesRetainTheSubmittingSession() {
		ThreadStore.setSessionId(" browser-session ");
		AgentRunRequest request = request();
		ThreadStore.remove();

		assertEquals("browser-session", request.getExecutionSessionId());
		assertEquals("browser-session", request.withParentRunId("parent").getExecutionSessionId());
		assertEquals("browser-session",
				request.withCompletionMode(SubAgentRunCompletionMode.POST_AND_CONTINUE).getExecutionSessionId());
		assertEquals("browser-session",
				request.forContinuation("room", "child", "continue", null).getExecutionSessionId());
	}

	@Test
	void missingSubmittingSessionRemainsMissing() {
		ThreadStore.remove();
		assertNull(request().getExecutionSessionId());
	}

	private static AgentRunRequest request() {
		return new AgentRunRequest("room", "input", "model", "SEMOSS", "workspace", 10, 2, null, null,
				null);
	}
}
