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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.tcp.client.SocketClient;

public class AutomationRunRegistryUnitTests {

	@Test
	void registersAndCancelsRunWithoutStartingPython() {
		String runId = UUID.randomUUID().toString();
		Insight insight = new Insight();
		insight.setInsightId("automation-" + runId);

		AutomationRunRegistry.register(runId, insight, null);
		try {
			assertTrue(AutomationRunRegistry.requestCancellation(runId));
		} finally {
			AutomationRunRegistry.unregister(runId);
		}
	}

	@Test
	void rejectsDuplicateRegistration() {
		String runId = UUID.randomUUID().toString();
		Insight insight = new Insight();
		insight.setInsightId("automation-" + runId);

		AutomationRunRegistry.register(runId, insight, null);
		try {
			assertThrows(IllegalStateException.class,
					() -> AutomationRunRegistry.register(runId, insight, null));
		} finally {
			AutomationRunRegistry.unregister(runId);
		}
	}

	@Test
	void interruptsPythonWhenItAttachesAfterCancellation() {
		String runId = UUID.randomUUID().toString();
		String insightId = "automation-" + runId;
		Insight insight = new Insight();
		insight.setInsightId(insightId);
		PyTranslator translator = mock(PyTranslator.class);
		SocketClient socketClient = mock(SocketClient.class);
		when(translator.getSocketClient()).thenReturn(socketClient);

		AutomationRunRegistry.register(runId, insight, "job-id");
		try {
			assertTrue(AutomationRunRegistry.requestCancellation(runId));
			AutomationRunRegistry.attachPythonTranslator(runId, translator);

			verify(socketClient).interruptInsightJob(insightId, "job-id");
		} finally {
			AutomationRunRegistry.unregister(runId);
		}
	}
}
