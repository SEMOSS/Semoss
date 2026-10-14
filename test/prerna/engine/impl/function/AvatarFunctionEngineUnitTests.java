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

	/** Records avatar server calls instead of hitting the network. */
	private static class RecordingEngine extends AvatarFunctionEngine {
		final List<String> calls = new ArrayList<>();
		final List<Map<String, Object>> bodies = new ArrayList<>();
		final Map<String, String> responses = new HashMap<>();

		@Override
		protected String sendRequest(String method, String path, Map<String, Object> body) {
			calls.add(method + " " + path);
			bodies.add(body);
			String canned = responses.get(path);
			return canned != null ? canned : "{\"code\":0,\"msg\":\"ok\"}";
		}
	}

	private RecordingEngine engine;

	private static Properties baseProps() {
		Properties p = new Properties();
		p.setProperty("ENGINE", "test-id");
		p.setProperty("ENGINE_ALIAS", "Avatar");
		p.setProperty("FUNCTION_NAME", "avatar");
		p.setProperty("FUNCTION_DESCRIPTION", "");
		p.setProperty("SERVER_URL", "http://avatar-host:8010/");
		p.setProperty("DEFAULT_AVATAR_ID", "avatar-1");
		return p;
	}

	private static Map<String, Object> params(Object... kv) {
		Map<String, Object> m = new HashMap<>();
		for (int i = 0; i < kv.length; i += 2) {
			m.put((String) kv[i], kv[i + 1]);
		}
		return m;
	}

	private static RecordingEngine open(Properties p) throws Exception {
		RecordingEngine e = new RecordingEngine();
		e.setBasic(true); // skips folder initialization
		e.open(p);
		return e;
	}

	@BeforeEach
	void setUp() throws Exception {
		engine = open(baseProps());
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
		assertTrue(engine instanceof IFunctionEngine);
	}

	@Test
	void openRequiresServerUrl() {
		Properties p = baseProps();
		p.remove("SERVER_URL");
		assertThrows(IllegalArgumentException.class, () -> open(p));
		p.setProperty("SERVER_URL", "  ");
		assertThrows(IllegalArgumentException.class, () -> open(p));
	}

	@Test
	void openRejectsUnknownProvider() {
		Properties p = baseProps();
		p.setProperty("AVATAR_PROVIDER", "HEYGEN");
		assertThrows(IllegalArgumentException.class, () -> open(p));
	}

	@Test
	void openRejectsWav2LipAsAvatarModel() {
		Properties p = baseProps();
		p.setProperty("DEFAULT_AVATAR_MODEL", "wav2lip");
		assertThrows(IllegalArgumentException.class, () -> open(p));
	}

	@Test
	void defaultMetadataIsFilledIn() {
		assertNotNull(engine.getFunctionDescription());
		assertFalse(engine.getParameters().isEmpty());
		assertEquals(List.of("action"), engine.getRequiredParameters());
	}

	@Test
	void missingOrUnknownActionFails() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params()));
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "dance")));
	}

	@Test
	@SuppressWarnings("unchecked")
	void connectRelaysOfferAndReturnsRawAnswer() {
		engine.responses.put("/offer", "{\"sdp\":\"answer-sdp\",\"type\":\"answer\",\"sessionid\":\"s1\"}");
		Object out = engine.execute(params("action", "connect", "sdp", "offer-sdp"));
		assertEquals(List.of("POST /offer"), engine.calls);
		Map<String, Object> body = engine.bodies.get(0);
		assertEquals("offer-sdp", body.get("sdp"));
		assertEquals("offer", body.get("type"));
		assertEquals("avatar-1", body.get("avatar"));
		assertEquals("s1", ((Map<String, Object>) out).get("sessionid"));
		assertEquals("answer-sdp", ((Map<String, Object>) out).get("sdp"));
	}

	@Test
	void connectRequiresSdp() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "connect")));
	}

	@Test
	void speakAlwaysUsesEchoMode() {
		engine.execute(params("action", "speak", "sessionId", "s1", "text", "hello"));
		assertEquals(List.of("POST /human"), engine.calls);
		Map<String, Object> body = engine.bodies.get(0);
		assertEquals("s1", body.get("sessionid"));
		assertEquals("hello", body.get("text"));
		assertEquals("echo", body.get("type"));
		assertEquals(false, body.get("interrupt"));
	}

	@Test
	void speakCannotRequestChatMode() {
		engine.execute(params("action", "speak", "sessionId", "s1", "text", "hi", "type", "chat"));
		assertEquals("echo", engine.bodies.get(0).get("type"));
	}

	@Test
	void speakPassesInterruptFlag() {
		engine.execute(params("action", "speak", "sessionId", "s1", "text", "hi", "interrupt", "true"));
		assertEquals(true, engine.bodies.get(0).get("interrupt"));
	}

	@Test
	void speakRequiresSessionAndText() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "speak", "text", "hi")));
		assertThrows(IllegalArgumentException.class,
				() -> engine.execute(params("action", "speak", "sessionId", "s1")));
	}

	@Test
	void interruptAndIsSpeakingNeedSession() {
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "interrupt")));
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "is_speaking")));
		engine.execute(params("action", "interrupt", "sessionId", "s1"));
		engine.responses.put("/is_speaking", "{\"code\":0,\"msg\":\"ok\",\"data\":true}");
		Object speaking = engine.execute(params("action", "is_speaking", "sessionId", "s1"));
		assertEquals(List.of("POST /interrupt_talk", "POST /is_speaking"), engine.calls);
		assertEquals(true, speaking);
	}

	@Test
	void nonZeroCodeIsAnError() {
		engine.responses.put("/human", "{\"code\":-1,\"msg\":\"session not found\"}");
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> engine.execute(params("action", "speak", "sessionId", "bad", "text", "hi")));
		assertTrue(e.getMessage().contains("session not found"));
	}

	@Test
	void createAvatarDefaultsToMuseTalkAndRejectsWav2Lip() {
		engine.responses.put("/api/avatar/task", "{\"code\":0,\"msg\":\"ok\",\"data\":{\"task_id\":\"t1\"}}");
		Object out = engine.execute(params("action", "create_avatar", "avatarId", "a2", "videoPath", "/v/a.mp4"));
		Map<String, Object> body = engine.bodies.get(0);
		assertEquals("musetalk", body.get("model"));
		assertEquals("a2", body.get("avatar_id"));
		assertEquals("/v/a.mp4", body.get("video_path"));
		assertEquals("t1", ((Map<?, ?>) out).get("task_id"));
		assertThrows(IllegalArgumentException.class, () -> engine.execute(
				params("action", "create_avatar", "avatarId", "a2", "videoPath", "/v/a.mp4", "model", "wav2lip")));
	}

	@Test
	void createAvatarRequiresIdAndVideo() {
		assertThrows(IllegalArgumentException.class,
				() -> engine.execute(params("action", "create_avatar", "videoPath", "/v/a.mp4")));
		assertThrows(IllegalArgumentException.class,
				() -> engine.execute(params("action", "create_avatar", "avatarId", "a2")));
	}

	@Test
	void avatarStatusEncodesTaskIdInRoute() {
		engine.responses.put("/api/avatar/task/t%2F1", "{\"code\":0,\"msg\":\"ok\",\"data\":{\"status\":\"running\"}}");
		Object out = engine.execute(params("action", "avatar_status", "taskId", "t/1"));
		assertEquals(List.of("GET /api/avatar/task/t%2F1"), engine.calls);
		assertEquals("running", ((Map<?, ?>) out).get("status"));
		assertThrows(IllegalArgumentException.class, () -> engine.execute(params("action", "avatar_status")));
	}

	@Test
	void trailingSlashInServerUrlIsTrimmedAndHeadersParse() throws Exception {
		Properties p = baseProps();
		p.setProperty("HEADERS", "{\"X-Gateway-Key\":\"k\"}");
		RecordingEngine e = open(p);
		e.execute(params("action", "interrupt", "sessionId", "s1"));
		assertEquals(List.of("POST /interrupt_talk"), e.calls);
	}
}
