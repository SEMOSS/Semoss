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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.Constants;
import prerna.util.StaticModelMetadataCatalog;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Checks the rules on a model's prices: only its owners and admins change them,
 * they cannot be removed, and the catalog replaces them only with prices of its
 * own. Runs against an in-memory MODELMETADATA table, with the catalog and the
 * permission checks replaced with static mocks.
 */
class SecurityModelMetadataUtilsPricingUnitTests {

	private static final String MODEL = "model-id";
	private static final String CONTRACT_PRICES = "[{\"servingProvider\":\"openai\",\"modelId\":\"test-model\","
			+ "\"input\":1,\"output\":4}]";
	private static final String CATALOG_PRICES = "[{\"servingProvider\":\"openai\",\"modelId\":\"test-model\","
			+ "\"input\":2,\"output\":8}]";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<StaticModelMetadataCatalog> catalog;
	private MockedStatic<SecurityEngineUtils> engines;
	private MockedStatic<SecurityAdminUtils> admins;
	private final User user = mock(User.class);

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:model-prices-" + UUID.randomUUID());
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
		when(securityDb.getConnection()).thenReturn(connection);
		when(securityDb.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(securityDb);
		catalog = mockStatic(StaticModelMetadataCatalog.class);
		engines = mockStatic(SecurityEngineUtils.class);
		admins = mockStatic(SecurityAdminUtils.class);

		Map<String, Object> details = new HashMap<>();
		details.put(Constants.MODEL, "test-model");
		details.put(Constants.PRICING, CONTRACT_PRICES);
		SecurityModelMetadataUtils.upsertModelMetadata(MODEL, details);
	}

	@AfterEach
	void tearDown() throws Exception {
		for (MockedStatic<?> mocked : new MockedStatic<?>[] { admins, engines, catalog, registry }) {
			if (mocked != null) {
				mocked.close();
			}
		}
		if (connection != null) {
			connection.close();
		}
	}

	@Test
	void pricesCannotBeRemovedOnlyChanged() {
		for (Object removed : new Object[] { null, "", "[]", "[{\"input\":1}]" }) {
			Map<String, Object> update = new HashMap<>();
			update.put(Constants.PRICING, removed);
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityModelMetadataUtils.updateModelMetadata(MODEL, update));
			assertEquals("A model's prices cannot be removed, only changed", e.getMessage());
		}
		assertEquals(4L, outputRate());

		SecurityModelMetadataUtils.updateModelMetadata(MODEL, Map.of(Constants.PRICING, CATALOG_PRICES));

		assertEquals(8L, outputRate());
	}

	@Test
	void anyPriceButTheOneCostLimitsUseCanBeRemoved() {
		String withAzure = "[{\"servingProvider\":\"openai\",\"modelId\":\"test-model\",\"input\":1,\"output\":4},"
				+ "{\"servingProvider\":\"azure\",\"modelId\":\"test-model\",\"input\":2,\"output\":5}]";
		SecurityModelMetadataUtils.updateModelMetadata(MODEL, Map.of(Constants.PRICING, withAzure));

		// with no serving provider or maker set, cost limits use the first entry, so
		// the
		// Azure one can be removed
		SecurityModelMetadataUtils.updateModelMetadata(MODEL, Map.of(Constants.PRICING, CONTRACT_PRICES));
		assertEquals(1, ((List<?>) SecurityModelMetadataUtils.getModelMetadata(MODEL).get("pricing")).size());

		// but the one in use cannot, even with another price in its place
		String azureOnly = "[{\"servingProvider\":\"azure\",\"modelId\":\"test-model\",\"input\":2,\"output\":5}]";
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityModelMetadataUtils.updateModelMetadata(MODEL, Map.of(Constants.PRICING, azureOnly)));
		assertEquals("The price cost limits count this model's calls at cannot be removed, only changed",
				e.getMessage());
		assertEquals(4L, outputRate());
	}

	@Test
	void onlyOwnersAndAdminsChangePrices() {
		engines.when(() -> SecurityEngineUtils.userIsOwner(any(), any())).thenReturn(false);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityModelMetadataUtils
				.updateModelMetadata(user, MODEL, Map.of(Constants.PRICING, CATALOG_PRICES)));
		assertEquals("Only the model's owners can change its prices", e.getMessage());
		// editors keep every other key
		assertDoesNotThrow(
				() -> SecurityModelMetadataUtils.updateModelMetadata(user, MODEL, Map.of(Constants.MAX_TOKENS, 4096L)));
		assertEquals(4L, outputRate());

		admins.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
		SecurityModelMetadataUtils.updateModelMetadata(user, MODEL, Map.of(Constants.PRICING, CATALOG_PRICES));
		assertEquals(8L, outputRate());

		admins.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);
		engines.when(() -> SecurityEngineUtils.userIsOwner(user, MODEL)).thenReturn(true);
		SecurityModelMetadataUtils.updateModelMetadata(user, MODEL, Map.of(Constants.PRICING, CONTRACT_PRICES));
		assertEquals(4L, outputRate());
	}

	@Test
	void aCatalogSyncKeepsPricesWhenTheCatalogHasNone() {
		Map<String, Object> withoutPrices = new LinkedHashMap<>();
		withoutPrices.put(Constants.MODEL_PROVIDER, "OPENAI");
		withoutPrices.put(Constants.PRICING, null);
		catalog.when(() -> StaticModelMetadataCatalog.getStaticDefaults("test-model"))
				.thenAnswer(call -> new LinkedHashMap<>(withoutPrices));

		SecurityModelMetadataUtils.syncModelMetadataFromCatalog(List.of(MODEL), true, false);

		assertEquals(4L, outputRate());
		assertEquals("OPENAI", SecurityModelMetadataUtils.getModelMetadata(MODEL).get("modelProvider"));
	}

	@Test
	void aCatalogSyncReplacesPricesWithItsOwn() {
		catalog.when(() -> StaticModelMetadataCatalog.getStaticDefaults("test-model"))
				.thenAnswer(call -> new LinkedHashMap<>(Map.of(Constants.PRICING, CATALOG_PRICES)));

		SecurityModelMetadataUtils.syncModelMetadataFromCatalog(List.of(MODEL), true, false);

		assertEquals(8L, outputRate());
	}

	/**
	 * @return the stored output rate of the model's one price entry
	 */
	private static Object outputRate() {
		List<?> pricing = (List<?>) SecurityModelMetadataUtils.getModelMetadata(MODEL).get("pricing");
		return ((Map<?, ?>) pricing.get(0)).get("output");
	}
}
