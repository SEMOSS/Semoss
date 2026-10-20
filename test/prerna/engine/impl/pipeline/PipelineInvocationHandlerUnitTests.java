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
package prerna.engine.impl.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.reactor.interceptor.PipelineReactorUtils;

class PipelineInvocationHandlerUnitTests {

	@Test
	void inputAuditPayloadIsCapturedBeforeLaterMutation() throws Exception {
		Method method = AuditTarget.class.getDeclaredMethod("execute", String.class, Map.class);
		Map<String, Object> parameters = new LinkedHashMap<>();
		parameters.put("threshold", 0.8);

		String snapshot = PipelineInvocationHandler.serializeAuditPayload(method,
				new Object[] { "masked input", parameters }, null, false);
		parameters.put("interim_result", Map.of("pass", true));
		parameters.put("result", "late response");

		JsonObject payload = JsonParser.parseString(snapshot).getAsJsonObject();
		assertEquals(2, payload.size());
		assertFalse(payload.has(PipelineReactorUtils.RESULT));
		assertFalse(snapshot.contains("late response"));
		assertFalse(snapshot.contains(PipelineReactorUtils.INTERIM_RESULT));
	}

	@Test
	void outputAuditPayloadContainsOnlyArgumentsAndForwardedResult() throws Exception {
		Method method = AuditTarget.class.getDeclaredMethod("execute", String.class, Map.class);

		String snapshot = PipelineInvocationHandler.serializeAuditPayload(method,
				new Object[] { "masked input", Map.of("threshold", 0.8) }, "guarded output", true);

		JsonObject payload = JsonParser.parseString(snapshot).getAsJsonObject();
		assertEquals(3, payload.size());
		assertEquals("guarded output", payload.get(PipelineReactorUtils.RESULT).getAsString());
		assertFalse(payload.has(PipelineReactorUtils.CONFIG));
		assertFalse(payload.has(PipelineReactorUtils.INTERIM_RESULT));
		assertTrue(snapshot.contains("masked input"));
	}

	private static final class AuditTarget {

		@SuppressWarnings("unused")
		private String execute(String input, Map<String, Object> parameters) {
			return input;
		}
	}
}
