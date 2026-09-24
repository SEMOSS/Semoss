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
package prerna.reactor.automation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;

/** Covers lookup and cleanup of ephemeral Insights owned by local runs. */
public class AutomationPythonRunRegistryUnitTests {

	@Test
	void insightIdExistsOnlyWhileRunIsRegistered() {
		String runId = "run-registry-test";
		Insight insight = new Insight();
		insight.setInsightId("insight-registry-test");

		try {
			AutomationPythonRunRegistry.register(runId, new PyTranslator(null, insight), insight, null);
			assertEquals("insight-registry-test", AutomationPythonRunRegistry.getInsightId(runId));
		} finally {
			AutomationPythonRunRegistry.unregister(runId);
		}

		assertNull(AutomationPythonRunRegistry.getInsightId(runId));
	}

	@Test
	void completedInsightIsRetainedUntilReleased() {
		String runId = "run-retention-test";
		Insight insight = new Insight();
		insight.setInsightId("insight-retention-test");
		AtomicBoolean cleaned = new AtomicBoolean();

		AutomationPythonRunRegistry.register(runId, new PyTranslator(null, insight), insight, null);
		AutomationPythonRunRegistry.retainInsightForInspection(runId, () -> cleaned.set(true));

		assertEquals("insight-retention-test", AutomationPythonRunRegistry.getInsightId(runId));
		assertFalse(cleaned.get());

		AutomationPythonRunRegistry.unregister(runId);
		assertNull(AutomationPythonRunRegistry.getInsightId(runId));
		assertTrue(cleaned.get());
	}
}
