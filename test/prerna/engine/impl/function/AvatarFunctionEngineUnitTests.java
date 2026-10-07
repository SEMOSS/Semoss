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
package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;

public class AvatarFunctionEngineUnitTests {

	/** Records provider calls instead of hitting the network. */
	private static class RecordingEngine extends AvatarFunctionEngine {
		final List<String> calls = new ArrayList<>();
		final List<Map<String, Object>> bodies = new ArrayList<>();
		String createResponse = "{\"data\":{\"session_id\":\"s1\",\"url\":\"wss://x\",\"access_token\":\"t\"}}";

		@Override
		protected String sendRequest(String method, String path, Map<String, Object> body) {
			calls.add(method + " " + path);
			bodies.add(body);
			if (path.contains("streaming.new")) {
				return createResponse;
			}
			return "{\"code\":100}";
		}
	}

	private RecordingEngine engine;

	private static Properties baseProps() {
		Properties p = new Properties();
		p.setProperty("ENGINE", "test-id");
		p.setProperty("ENGINE_ALIAS", "Avatar");
		p.setProperty("FUNCTION_NAME", "avatar");
		p.setProperty("FUNCTION_DESCRIPTION", "");
		p.setProperty("API_KEY", "secret-key");
		p.setProperty("DEFAULT_AVATAR_ID", "avatar-1");
		p.setProperty("DEFAULT_VOICE_ID", "voice-1");
		return p;
	}

	private static Map<String, Object> params(Object... kv) {
		Map<String, Object> m = new HashMap<>();
		for (int i = 0; i < kv.length; i += 2) {
			m.put((String) kv[i], kv[i + 1]);
		}
		return m;
	}

	@BeforeEach
	void setUp() throws Exception {
		engine = new RecordingEngine();
		engine.setBasic(true); // skips folder initialization
		engine.open(baseProps());
	}

	@Test
	void enumMapsToEngine() {
		assertEquals("AVATAR", FunctionTypeEnum.AVATAR.getFunctionName());
		assertEquals(AvatarFunctionEngine.class.getName(), FunctionTypeEnum.AVATAR.getFunctionClass());
		assertEquals(FunctionTypeEnum.AVATAR, FunctionTypeEnum.getEnumFromName("avatar"));
	}

	@Test
	void catalogTypeAndSubtype() {
		assertEquals(IEngine.CATALOG_TYPE.FUNCTION, engine.getCatalogType());
		assertEquals("AVATAR", engine.getCatalogSubType(baseProps()));
	}

	@Test
	void openRequiresApiKey() {
		Properties p = baseProps();
		p.remove("API_KEY");
		assertThrows(IllegalArgumentException.class, () -> {
			RecordingEngine e = new RecordingEngine();
			e.setBasic(true);
			e.open(p);
		});
	}

	@Test
	void openRejectsUnknownProvider() {
		Properties p = baseProps();
		p.setProperty("AVATAR_PROVIDER", "NOPE");
		assertThrows(IllegalArgumentException.class, () -> {
			RecordingEngine e = new RecordingEngine();
			e.setBasic(true);
			e.open(p);
		});
	}

	@Test
	void defaultMetadataIsFilledIn() {
		assertNotNull(engine.getFunctionDescription());
		assertFalse(engine.getParameters().isEmpty());
		assertEquals(List.of("action"), engine.getRequiredParameters());
	}

	@Test
	void missingActionFails() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params()));
	}

	@Test
	void unknownActionFails() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "dance")));
	}

	@Test
	@SuppressWarnings("unchecked")
	void createSessionUsesDefaultsAndStartsStream() {
		Object out = engine.execute(params("action", "create_session"));
		assertEquals(List.of("POST /v1/streaming.new", "POST /v1/streaming.start"), engine.calls);
		Map<String, Object> newBody = engine.bodies.get(0);
		assertEquals("avatar-1", newBody.get("avatar_id"));
		assertEquals("medium", newBody.get("quality"));
		assertEquals("voice-1", ((Map<String, Object>) newBody.get("voice")).get("voice_id"));
		assertEquals("s1", engine.bodies.get(1).get("session_id"));
		assertTrue(out instanceof Map);
	}

	@Test
	void createSessionNeverLeaksApiKey() {
		Object out = engine.execute(params("action", "create_session"));
		assertFalse(out.toString().contains("secret-key"));
	}

	@Test
	void speakSendsTextAndDefaultsToRepeat() {
		engine.execute(params("action", "speak", "sessionId", "s1", "text", "hello"));
		assertEquals(List.of("POST /v1/streaming.task"), engine.calls);
		Map<String, Object> body = engine.bodies.get(0);
		assertEquals("hello", body.get("text"));
		assertEquals("repeat", body.get("task_type"));
	}

	@Test
	void speakRequiresSessionAndText() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "speak", "text", "hi")));
		assertThrows(IllegalArgumentException.class,
				() -> engine.execute(params("action", "speak", "sessionId", "s1")));
	}

	@Test
	void speakRejectsBadTaskType() {
		assertThrows(IllegalArgumentException.class,
				() -> engine.execute(params("action", "speak", "sessionId", "s1", "text", "hi", "taskType", "x")));
	}

	@Test
	void interruptAndCloseNeedSession() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "interrupt")));
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "close_session")));
		engine.execute(params("action", "interrupt", "sessionId", "s1"));
		engine.execute(params("action", "close_session", "sessionId", "s1"));
		assertEquals(List.of("POST /v1/streaming.interrupt", "POST /v1/streaming.stop"), engine.calls);
	}

	@Test
	void listActionsUseGet() {
		engine.execute(params("action", "list_avatars"));
		engine.execute(params("action", "list_voices"));
		assertEquals(List.of("GET /v1/streaming/avatar.list", "GET /v2/voices"), engine.calls);
	}

	@Test
	void functionDefinitionJsonHasName() {
		engine.setFunctionName("avatar");
		assertEquals("avatar", engine.getFunctionDefintionJson().getString("name"));
	}

	@Test
	void ifaceIsFunctionEngine() {
		assertTrue(engine instanceof IFunctionEngine);
	}
}
