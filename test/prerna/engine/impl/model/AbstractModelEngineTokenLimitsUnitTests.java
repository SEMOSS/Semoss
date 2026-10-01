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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;

class AbstractModelEngineTokenLimitsUnitTests {

	@Test
	void savedLimitsOverrideLegacySmssWithoutChangingTheOriginalProperties() throws Exception {
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
			metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id"))
					.thenReturn(Map.of("contextWindow", 1_000_000L, "maxOutputTokens", 64_000L));
			TestModelEngine engine = new TestModelEngine();
			engine.open(legacyProperties());

			assertEquals(1_000_000, engine.getContextWindow());
			assertEquals(64_000L, engine.maxTokens);
			assertEquals("1000000", engine.getSmssProp().getProperty(Constants.CONTEXT_WINDOW));
			assertEquals("64000", engine.getSmssProp().getProperty(Constants.MAX_TOKENS));
			assertEquals("128000", engine.getOrigSmssProp().getProperty(Constants.CONTEXT_WINDOW));
			assertEquals("4096", engine.getOrigSmssProp().getProperty(Constants.MAX_TOKENS));
		}
	}

	@Test
	void reloadingPicksUpSavedChangesAndClearsWithoutRestoringLegacyLimits() throws Exception {
		Map<String, Object> row = new HashMap<>(Map.of("contextWindow", 256_000L, "maxOutputTokens", 16_000L));
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
			metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id")).thenReturn(row);
			TestModelEngine engine = new TestModelEngine();
			engine.open(legacyProperties());
			assertEquals(256_000, engine.getContextWindow());

			row.put("contextWindow", 512_000L);
			row.put("maxOutputTokens", 32_000L);
			engine.open(engine.getOrigSmssProp());
			assertEquals(512_000, engine.getContextWindow());
			assertEquals(32_000L, engine.maxTokens);

			row.put("contextWindow", null);
			row.put("maxOutputTokens", null);
			engine.open(engine.getOrigSmssProp());
			assertEquals(0, engine.getContextWindow());
			assertNull(engine.maxTokens);
			assertFalse(engine.getSmssProp().containsKey(Constants.CONTEXT_WINDOW));
			assertFalse(engine.getSmssProp().containsKey(Constants.MAX_TOKENS));
		}
	}

	@Test
	void modelsWithoutAMetadataRowRetainCaseInsensitiveLegacyLimits() throws Exception {
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
			metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id")).thenReturn(null);
			Properties properties = legacyProperties();
			properties.remove(Constants.CONTEXT_WINDOW);
			properties.remove(Constants.MAX_TOKENS);
			properties.setProperty("context_window", " 128000 ");
			properties.setProperty("max_tokens", "4096");
			TestModelEngine engine = new TestModelEngine();
			engine.open(properties);
			assertEquals(128_000, engine.getContextWindow());
			assertEquals(4096L, engine.maxTokens);
		}
	}

	@Test
	void failedMetadataReadDoesNotSilentlyUseStaleSmssLimits() {
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
			IllegalArgumentException failure = new IllegalArgumentException("Database unavailable");
			metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id")).thenThrow(failure);
			assertSame(failure, assertThrows(IllegalArgumentException.class,
					() -> new TestModelEngine().open(legacyProperties())));
		}
	}

	private static Properties legacyProperties() {
		Properties properties = new Properties();
		properties.setProperty(Constants.ENGINE, "model-id");
		properties.setProperty(Constants.ENGINE_ALIAS, "test-model");
		properties.setProperty(Constants.CONTEXT_WINDOW, "128000");
		properties.setProperty(Constants.MAX_TOKENS, "4096");
		return properties;
	}

	private static class TestModelEngine extends AbstractModelEngine {
		TestModelEngine() {
			setBasic(true);
		}

		@Override
		protected AskModelEngineResponse askCall(InputMessage message, Insight insight, String roomId,
				Map<String, Object> parameters) {
			return null;
		}

		@Override
		protected EmbeddingsModelEngineResponse embeddingsCall(List<String> strings, Insight insight,
				Map<String, Object> parameters) {
			return null;
		}

		@Override
		public ModelTypeEnum getModelType() {
			return ModelTypeEnum.OPEN_AI;
		}

		@Override
		public void close() {
		}
	}
}
