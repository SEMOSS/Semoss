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
 ******************************************************************************/
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

class GetAutomationRunReactorUnitTests {

	@Test
	void identifiesFramesOnlyFromInternalOutputKind() {
		Map<String, Object> ordinaryJson = Map.of(AutomationConstants.OUTPUT_VALUE,
				"{\"dataType\":\"table\",\"rowCount\":2,\"columnCount\":1,\"label\":\"ordinary JSON\"}");
		Map<String, Object> frame = Map.of(AutomationConstants.OUTPUT_KIND,
				AutomationConstants.OUTPUT_KIND_FRAME);

		assertFalse(GetAutomationRunReactor.isFrameOutput(ordinaryJson));
		assertTrue(GetAutomationRunReactor.isFrameOutput(frame));
	}

	@Test
	void marksExpiredFrameHistoryAsUnavailable() {
		Map<String, Object> frameOutput = Map.of(AutomationConstants.OUTPUT_KIND,
				AutomationConstants.OUTPUT_KIND_FRAME, AutomationConstants.OUTPUT_VAR_NAME, "query_result");
		Map<String, Object> historyResult = new HashMap<>();

		new GetAutomationRunReactor().decorateFrameResult(null, "project", frameOutput, historyResult);

		assertEquals(true, historyResult.get("OUTPUT_FRAME_UNAVAILABLE"));
		assertEquals("Frame data is unavailable because this run's execution workspace is closed.",
				historyResult.get(AutomationConstants.OUTPUT_PREVIEW));
	}
}
