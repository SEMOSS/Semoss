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
package prerna.reactor.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUsageLimitUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.usage.UsageLimit;
import prerna.engine.impl.usage.UsageLimitKey;
import prerna.engine.impl.usage.UsageRule;
import prerna.engine.impl.usage.UsageWindows;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;

class DatabaseUsageLimitReactorUnitTests {
	private static SetDatabaseUsageLimitReactor reactor(User actor, Object config) {
		var reactor = new SetDatabaseUsageLimitReactor();
		var insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(actor);
		reactor.setInsight(insight);
		var store = new NounStore("usage-test");
		if (config instanceof String json)
			store.makeNoun("config").addLiteral(json);
		else if (config != null)
			store.makeNoun("config").add(config, PixelDataType.MAP);
		reactor.setNounStore(store);
		return reactor;
	}

	private static final String CONFIG = """
			{"key":{"catalog":"DATABASE","source":"ENGINE_TOTAL","engineId":"db"},"usageLimit":null,"admin":true,"userId":"untrusted"}
			""";

	@Test
	void ownerWriteUsesInsightIdentityAndIgnoresClientAdminClaims() throws Exception {
		var actor = mock(User.class);
		var key = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.ENGINE_TOTAL, "db", null, null);
		try (var admins = mockStatic(SecurityAdminUtils.class);
				var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			var result = reactor(actor, CONFIG).execute();
			assertEquals(PixelDataType.MAP, result.getNounType());
			policies.verify(() -> SecurityEngineUsageLimitUtils.setLimit(actor, key, null));
			policies.verifyNoMoreInteractions();
			assertTrue(((Map<?, ?>) result.getValue()).containsKey("usageLimit"));
			assertNull(((Map<?, ?>) result.getValue()).get("usageLimit"));
		}
	}

	@Test
	void mapConfigurationUsesTheSameTypedContractAsJsonText() throws Exception {
		var actor = mock(User.class);
		var key = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.ENGINE_TOTAL, "db", null, null);
		var limit = new UsageLimit(false, List
				.of(new UsageRule(UsageRule.Metric.ROWS, 5, UsageWindows.Window.DAY, null, UsageRule.Counts.SHARED)));
		var config = Map.of("key", Map.of("catalog", "DATABASE", "source", "ENGINE_TOTAL", "engineId", "db"),
				"usageLimit", Map.of("unlimited", false, "rules",
						List.of(Map.of("metric", "ROWS", "amount", 5, "window", "DAY", "counts", "SHARED"))));
		try (var admins = mockStatic(SecurityAdminUtils.class);
				var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			assertEquals(limit, ((Map<?, ?>) reactor(actor, config).execute().getValue()).get("usageLimit"));
			policies.verify(() -> SecurityEngineUsageLimitUtils.setLimit(actor, key, limit));
		}
	}

	@Test
	void aReadEntryCanBeSavedAgainWithoutClearingItsLimit() throws Exception {
		var actor = mock(User.class);
		var key = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.ENGINE_TOTAL, "db", null, null);
		var limit = new UsageLimit(false, List
				.of(new UsageRule(UsageRule.Metric.ROWS, 5, UsageWindows.Window.DAY, null, UsageRule.Counts.SHARED)));
		var entry = new SecurityEngineUsageLimitUtils.Entry(key, limit);
		try (var admins = mockStatic(SecurityAdminUtils.class);
				var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			assertEquals(limit, ((Map<?, ?>) reactor(actor, entry).execute().getValue()).get("usageLimit"));
			policies.verify(() -> SecurityEngineUsageLimitUtils.setLimit(actor, key, limit));
		}
	}

	@Test
	void fractionalAmountsAreNotSilentlyTruncatedFromMapInputs() {
		var config = Map.of("key", Map.of("catalog", "DATABASE", "source", "ENGINE_TOTAL", "engineId", "db"),
				"usageLimit", Map.of("unlimited", false, "rules",
						List.of(Map.of("metric", "ROWS", "amount", 5.5, "window", "DAY", "counts", "SHARED"))));
		try (var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			assertThrows(IllegalArgumentException.class, () -> reactor(mock(User.class), config).execute());
			policies.verifyNoInteractions();
		}
	}

	@Test
	void adminWritesRequireServerObtainedProof() throws Exception {
		var actor = mock(User.class);
		var admin = mock(SecurityAdminUtils.class);
		var key = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.ENGINE_TOTAL, "db", null, null);
		try (var admins = mockStatic(SecurityAdminUtils.class);
				var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			admins.when(() -> SecurityAdminUtils.getInstance(actor)).thenReturn(admin);
			reactor(actor, CONFIG).execute();
			policies.verify(() -> SecurityEngineUsageLimitUtils.setAdminLimit(admin, actor, key, null));
			policies.verifyNoMoreInteractions();
		}
	}

	@Test
	void malformedOrMissingConfigHasNoSideEffects() {
		try (var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			assertThrows(IllegalArgumentException.class, () -> reactor(mock(User.class), null).execute());
			assertThrows(IllegalArgumentException.class, () -> reactor(mock(User.class), "not-json").execute());
			assertThrows(IllegalArgumentException.class, () -> reactor(mock(User.class), "{}").execute());
			policies.verifyNoInteractions();
		}
	}

	@Test
	void ownerReadDelegatesWithTheAuthenticatedActor() {
		var actor = mock(User.class);
		var insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(actor);
		var reactor = new GetDatabaseUsageLimitsReactor();
		reactor.setInsight(insight);
		var store = new NounStore("usage-test");
		store.makeNoun("database").addLiteral("db");
		reactor.setNounStore(store);
		try (var policies = mockStatic(SecurityEngineUsageLimitUtils.class)) {
			policies.when(() -> SecurityEngineUsageLimitUtils.getEngineLimits(actor, "db")).thenReturn(List.of());
			assertEquals(List.of(), reactor.execute().getValue());
			policies.verify(() -> SecurityEngineUsageLimitUtils.getEngineLimits(actor, "db"));
		}
	}
}
