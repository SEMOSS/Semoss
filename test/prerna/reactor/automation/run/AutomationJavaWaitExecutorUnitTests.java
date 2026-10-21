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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.om.Insight;

class AutomationJavaWaitExecutorUnitTests {

	private String registeredRunId;

	@AfterEach
	void cleanup() {
		if (registeredRunId != null) {
			AutomationRunRegistry.unregister(registeredRunId);
		}
	}

	@Test
	void returnsLegacyCompatibleOutputWithoutPython() {
		register("run-output");

		Map<String, Object> result = AutomationJavaWaitExecutor.execute(registeredRunId, 0.01);

		assertEquals(Map.of("waitedSeconds", 0.01), result);
	}

	@Test
	void rejectsDurationOutsideValidatedBounds() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationJavaWaitExecutor.execute("unregistered", Double.NaN));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationJavaWaitExecutor.execute("unregistered", 3600.01));
	}

	@Test
	void cancellationBeforeAttachmentStopsWaitImmediately() throws Exception {
		register("run-cancelled");
		AutomationRunRegistry.requestCancellation(registeredRunId);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<Map<String, Object>> wait = executor.submit(
					() -> AutomationJavaWaitExecutor.execute(registeredRunId, 60));
			ExecutionException error = assertThrows(ExecutionException.class,
					() -> wait.get(2, TimeUnit.SECONDS));
			assertEquals("Run cancelled by user", error.getCause().getMessage());
		} finally {
			executor.shutdownNow();
		}
	}

	@Test
	void activeCancellationStopsWaitImmediately() throws Exception {
		register("run-active");
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<Map<String, Object>> wait = executor.submit(
					() -> AutomationJavaWaitExecutor.execute(registeredRunId, 60));
			Thread.sleep(20);
			AutomationRunRegistry.requestCancellation(registeredRunId);
			ExecutionException error = assertThrows(ExecutionException.class,
					() -> wait.get(2, TimeUnit.SECONDS));
			assertEquals("Run cancelled by user", error.getCause().getMessage());
		} finally {
			executor.shutdownNow();
		}
	}

	private void register(String runId) {
		Insight insight = mock(Insight.class);
		when(insight.getInsightId()).thenReturn("insight");
		AutomationRunRegistry.register(runId, insight, null);
		registeredRunId = runId;
	}
}
