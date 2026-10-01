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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.Constants;
import prerna.util.StaticModelMetadataCatalog;
import prerna.util.SystemEngineRegistry;

class SecurityModelMetadataUtilsTokenLimitsUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<StaticModelMetadataCatalog> catalog;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:token-limits-" + UUID.randomUUID());
		try (Statement statement = connection.createStatement()) {
			statement.execute("""
					CREATE TABLE MODELMETADATA (
					ENGINEID VARCHAR PRIMARY KEY, MODELID VARCHAR, CATALOGMODELKEY VARCHAR,
					MODELPROVIDER VARCHAR, SERVINGPROVIDER VARCHAR, CAPABILITY VARCHAR, FAMILY VARCHAR,
					INPUTMODALITIES CLOB, OUTPUTMODALITIES CLOB, CONTEXTWINDOW BIGINT, MAXOUTPUTTOKENS BIGINT,
					BUILTINTOOLS CLOB, ATTACHMENT BOOLEAN, REASONING BOOLEAN, TOOLCALL BOOLEAN,
					STRUCTUREDOUTPUT BOOLEAN, TEMPERATURE BOOLEAN, KNOWLEDGECUTOFF VARCHAR,
					RELEASEDATE VARCHAR, SUPPORTEDPARAMETERS CLOB, REASONINGCONFIG CLOB,
					BENCHMARKS CLOB, PRICING CLOB)
					""");
		}
		IRDBMSEngine securityDb = mock(IRDBMSEngine.class);
		when(securityDb.getPreparedStatement(anyString()))
				.thenAnswer(invocation -> connection.prepareStatement(invocation.getArgument(0, String.class)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(securityDb);
		catalog = mockStatic(StaticModelMetadataCatalog.class);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (catalog != null) catalog.close();
		if (registry != null) registry.close();
		if (connection != null) connection.close();
	}

	@Test
	void startupCatalogingCannotOverwriteSavedLimitsWithLegacySmss() {
		SecurityModelMetadataUtils.upsertModelMetadata("model-id", legacyProperties());
		SecurityModelMetadataUtils.updateModelMetadata("model-id",
				Map.of(Constants.CONTEXT_WINDOW, 1_000_000L, Constants.MAX_TOKENS, 64_000L));

		SecurityModelMetadataUtils.upsertModelMetadata("model-id", legacyProperties());

		Map<String, Object> saved = SecurityModelMetadataUtils.getModelMetadata("model-id");
		assertEquals(1_000_000L, saved.get("contextWindow"));
		assertEquals(64_000L, saved.get("maxOutputTokens"));
		assertEquals("test-model", saved.get("modelId"));
	}

	@Test
	void clearedLimitsSurviveSmssAndAutomaticCatalogRefresh() {
		SecurityModelMetadataUtils.upsertModelMetadata("model-id", legacyProperties());
		Map<String, Object> cleared = new HashMap<>();
		cleared.put(Constants.CONTEXT_WINDOW, null);
		cleared.put(Constants.MAX_TOKENS, null);
		SecurityModelMetadataUtils.updateModelMetadata("model-id", cleared);
		catalog.when(() -> StaticModelMetadataCatalog.applyStaticDefaults(anyMap())).thenAnswer(invocation -> {
			Map<String, Object> details = invocation.getArgument(0);
			details.put(Constants.CONTEXT_WINDOW, 256_000L);
			details.put(Constants.MAX_TOKENS, 16_000L);
			return null;
		});

		SecurityModelMetadataUtils.upsertModelMetadata("model-id", legacyProperties());

		Map<String, Object> saved = SecurityModelMetadataUtils.getModelMetadata("model-id");
		assertNull(saved.get("contextWindow"));
		assertNull(saved.get("maxOutputTokens"));
	}

	@Test
	void legacyLimitsSeedModelsThatDoNotHaveAMetadataRow() {
		assertNull(SecurityModelMetadataUtils.getModelMetadata("model-id"));
		SecurityModelMetadataUtils.upsertModelMetadata("model-id", legacyProperties());
		Map<String, Object> saved = SecurityModelMetadataUtils.getModelMetadata("model-id");
		assertEquals(128_000L, saved.get("contextWindow"));
		assertEquals(4096L, saved.get("maxOutputTokens"));
	}

	@Test
	void newSmssPropertiesExcludeLimitsWhilePreservingRuntimeConfiguration() {
		Map<String, Object> details = Map.of(Constants.MODEL, "test-model", "INIT_MODEL_ENGINE", "build_model()",
				"context_window", 128_000L, Constants.MAX_TOKENS, 4096L, "API_KEY", "test-key");

		Map<String, Object> properties = SecurityModelMetadataUtils.getModelEngineProperties(details);

		assertEquals(Map.of(Constants.MODEL, "test-model", "INIT_MODEL_ENGINE", "build_model()",
				"API_KEY", "test-key"), properties);
		assertTrue(details.containsKey("context_window"));
		assertTrue(details.containsKey(Constants.MAX_TOKENS));
	}

	private static Properties legacyProperties() {
		Properties properties = new Properties();
		properties.setProperty(Constants.MODEL, "test-model");
		properties.setProperty(Constants.CONTEXT_WINDOW, "128000");
		properties.setProperty(Constants.MAX_TOKENS, "4096");
		return properties;
	}
}
