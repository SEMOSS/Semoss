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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AutomationPixelExecutorUnitTests {

	private static final String RUN_ID = "pixel-run";

	@AfterEach
	void cleanup() {
		AutomationRunRegistry.unregister(RUN_ID);
	}

	@Test
	void returnsLastSuccessfulPixelValue() {
		Insight insight = registeredInsight();
		PixelRunner runner = mock(PixelRunner.class);
		when(insight.getPixelRunner()).thenReturn(runner);
		when(insight.runPixel(runner, "META | GetUserInfo();")).thenReturn(runner);
		when(runner.getResults()).thenReturn(List.of(
				new NounMetadata(Map.of("name", "user"), PixelDataType.MAP, PixelOperationType.OPERATION)));

		Object result = new AutomationPixelExecutor(insight, RUN_ID).execute("META | GetUserInfo();");

		assertEquals(Map.of("name", "user"), result);
	}

	@Test
	void surfacesPixelErrorResult() {
		Insight insight = registeredInsight();
		PixelRunner runner = mock(PixelRunner.class);
		when(insight.getPixelRunner()).thenReturn(runner);
		when(insight.runPixel(runner, "Bad();")).thenReturn(runner);
		when(runner.getResults()).thenReturn(List.of(
				new NounMetadata("bad pixel", PixelDataType.CONST_STRING, PixelOperationType.ERROR)));

		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> new AutomationPixelExecutor(insight, RUN_ID).execute("Bad();"));
		assertEquals("bad pixel", error.getMessage());
	}

	@Test
	void surfacesEstablishedAndAdditionalPixelErrors() {
		Insight insight = registeredInsight();
		PixelRunner runner = mock(PixelRunner.class);
		when(insight.getPixelRunner()).thenReturn(runner);
		when(insight.runPixel(runner, "NeedsInput();")).thenReturn(runner);
		NounMetadata result = new NounMetadata("waiting", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		result.addAdditionalReturn(new NounMetadata("input required", PixelDataType.CONST_STRING,
				PixelOperationType.USER_INPUT_REQUIRED));
		when(runner.getResults()).thenReturn(List.of(result));

		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> new AutomationPixelExecutor(insight, RUN_ID).execute("NeedsInput();"));
		assertEquals("waiting", error.getMessage());
	}

	@Test
	void rejectsFrameResultInsteadOfCoercingIt() {
		Insight insight = registeredInsight();
		PixelRunner runner = mock(PixelRunner.class);
		when(insight.getPixelRunner()).thenReturn(runner);
		when(insight.runPixel(runner, "Frame();")).thenReturn(runner);
		when(runner.getResults()).thenReturn(List.of(
				new NounMetadata(new Object(), PixelDataType.FRAME, PixelOperationType.FRAME)));

		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> new AutomationPixelExecutor(insight, RUN_ID).execute("Frame();"));
		assertEquals("Automation Pixel expression returned unsupported result type FRAME.", error.getMessage());
	}

	private static Insight registeredInsight() {
		Insight insight = mock(Insight.class);
		when(insight.getInsightId()).thenReturn("insight");
		AutomationRunRegistry.register(RUN_ID, insight, null);
		return insight;
	}
}
