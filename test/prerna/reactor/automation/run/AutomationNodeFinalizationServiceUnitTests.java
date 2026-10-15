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

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

class AutomationNodeFinalizationServiceUnitTests {

	@Test
	void classifiesGeneratedTraceEnvelopeAndPreparesBoundedValue() {
		Map<String, Object> node = Map.of(
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_MODEL_CHAT,
				AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		AutomationNodeFinalizationService.ClassifiedResult classified =
				AutomationNodeFinalizationService.classify(node, Map.of(
						AutomationConstants.INTERNAL_RESULT_VALUE, "answer",
						AutomationConstants.INTERNAL_RESULT_METADATA, Map.of("messageId", "message")));

		assertEquals("answer", classified.value());
		assertEquals(Map.of("messageId", "message"), classified.metadata());
		assertEquals("\"answer\"", AutomationNodeFinalizationService.prepare(
				"model", "answer", classified.value(), Map.of("input", 1), true).output());
	}

	@Test
	void rejectsIncompleteInternalEnvelope() {
		Map<String, Object> node = Map.of(
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_AGENT_RUN,
				AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		assertThrows(IllegalStateException.class,
				() -> AutomationNodeFinalizationService.classify(node,
						Map.of(AutomationConstants.INTERNAL_RESULT_VALUE, "partial")));
	}

	@Test
	void validatesLogicalScopeWithoutSerializingFrameInfrastructure() {
		AutomationFrameOutput.RegisteredFrame frame = new AutomationFrameOutput.RegisteredFrame(
				"heroes", null, Map.of("dataType", "table", "rowCount", 3L, "columnCount", 2), "PY");

		assertEquals("\"answer\"", AutomationNodeFinalizationService.prepare(
				"model", "answer", "answer", Map.of("heroes", frame), true).output());
	}
}
