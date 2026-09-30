/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

	@Test
	void transferMetadataSurvivesDurableRoundTrip() {
		AgentRunRequest transferred = request().withTransferMetadata("from-run", "root-run");

		AgentRunRequest restored = AgentRunRequest.fromPersistedMap(transferred.toPersistedMap(), null);

		assertEquals("from-run", restored.getTransferFromRunId());
		assertEquals("root-run", restored.getTransferRootRunId());
		assertEquals("from-run", restored.getParamMap().get(AgentRunRequest.PARAM_TRANSFER_FROM_RUN_ID));
		assertEquals("root-run", restored.getParamMap().get(AgentRunRequest.PARAM_TRANSFER_ROOT_RUN_ID));
	}

	private static AgentRunRequest request() {
		return new AgentRunRequest("room", "input", "model", "SEMOSS", "workspace", 10, 2, null, null,
				null);
	}
}
