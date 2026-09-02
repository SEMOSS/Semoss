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
package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

class GetUserModelCreditInfoReactorUnitTests {

	private GetUserModelCreditInfoReactor reactor;
	private User user;

	@BeforeEach
	void setup() {
		user = mock(User.class);
		AccessToken accessToken = mock(AccessToken.class);
		when(accessToken.getId()).thenReturn("current-user");
		when(user.getPrimaryLoginToken()).thenReturn(accessToken);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);

		reactor = new GetUserModelCreditInfoReactor();
		reactor.setInsight(insight);
		reactor.keyValue.put(ReactorKeysEnum.ENGINE.getKey(), "model-alias");
	}

	@Test
	void returnsConfiguredCreditBalanceAndSnapshotPricing() {
		Map<String, Object> permission = new HashMap<>();
		permission.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "credit");
		permission.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "month");
		permission.put(Constants.ENGINE_MAX_CREDIT_KEY, 10D);

		Map<String, Object> metadata = new HashMap<>();
		metadata.put("inputTokenCredit", 0.000002D);
		metadata.put("outputTokenCredit", 0.000008D);
		metadata.put("cacheReadMultiplier", 0.25D);
		ZonedDateTime now = ZonedDateTime.of(2026, 9, 1, 12, 0, 0, 0, ZoneOffset.UTC);

		try (MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SecurityModelMetadataUtils> metadataUtils = Mockito
						.mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "model-alias"))
					.thenReturn("model-id");
			engineUtils.when(() -> SecurityEngineUtils.userCanViewEngine(user, "model-id")).thenReturn(true);
			engineUtils.when(() -> SecurityEngineUtils.getEngineType("model-id"))
					.thenReturn(IEngine.CATALOG_TYPE.MODEL);
			engineUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMapForUserId("current-user", "model-id"))
					.thenReturn(List.of(permission));
			metadataUtils.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id")).thenReturn(metadata);
			utility.when(Utility::isModelInferenceLogsEnabled).thenReturn(true);
			utility.when(Utility::getCurrentZonedDateTimeUTC).thenReturn(now);
			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(eq("credit"),
					eq("current-user"),
					eq("model-id"), any(ZonedDateTime.class), eq("MONTH"))).thenReturn(4.5D);

			NounMetadata noun = reactor.execute();

			assertEquals(PixelDataType.MAP, noun.getNounType());
			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) noun.getValue();
			assertEquals("model-id", result.get("engineId"));
			assertEquals("current-user", result.get("userId"));
			assertEquals("MONTH", result.get("frequency"));
			assertEquals(10D, result.get("maxCredits"));
			assertEquals(4.5D, result.get("creditsUsed"));
			assertEquals(5.5D, result.get("creditsRemaining"));
			assertEquals(2D, result.get("inputCreditsPerMillion"));
			assertEquals(8D, result.get("outputCreditsPerMillion"));
			assertEquals(0.25D, result.get("cacheReadMultiplier"));
			assertEquals(1D, result.get("cacheWriteMultiplier"));
			assertTrue((Boolean) result.get("restrictionEnabled"));
			assertTrue((Boolean) result.get("pricingConfigured"));
			assertFalse((Boolean) result.get("limitExceeded"));
			assertEquals("2026-09-01T00:00Z", result.get("periodStart"));
			assertEquals("2026-09-30T23:59:59.999999999Z", result.get("periodEnd"));
		}
	}

	@Test
	void returnsConfigurationWhenUsageTrackingIsDisabled() {
		Map<String, Object> permission = new HashMap<>();
		permission.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "credit");
		permission.put(Constants.ENGINE_MAX_CREDIT_KEY, 5D);

		try (MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SecurityModelMetadataUtils> metadataUtils = Mockito
						.mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "model-alias"))
					.thenReturn("model-id");
			engineUtils.when(() -> SecurityEngineUtils.userCanViewEngine(user, "model-id")).thenReturn(true);
			engineUtils.when(() -> SecurityEngineUtils.getEngineType("model-id"))
					.thenReturn(IEngine.CATALOG_TYPE.MODEL);
			engineUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMapForUserId("current-user", "model-id"))
					.thenReturn(List.of(permission));
			metadataUtils.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id"))
					.thenReturn(Map.of());
			utility.when(Utility::isModelInferenceLogsEnabled).thenReturn(false);

			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) reactor.execute().getValue();

			assertTrue((Boolean) result.get("restrictionEnabled"));
			assertFalse((Boolean) result.get("trackingEnabled"));
			assertEquals("DAY", result.get("frequency"));
			assertEquals(5D, result.get("maxCredits"));
			assertNull(result.get("creditsUsed"));
			assertNull(result.get("creditsRemaining"));
			assertFalse((Boolean) result.get("pricingConfigured"));
		}
	}

	@Test
	void returnsUsageForCustomDateRangeWithoutApplyingLimitBalance() {
		Map<String, Object> permission = new HashMap<>();
		permission.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "credit");
		permission.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "day");
		permission.put(Constants.ENGINE_MAX_CREDIT_KEY, 10D);
		reactor.keyValue.put(ReactorKeysEnum.START_DATE.getKey(), "2026-08-01");
		reactor.keyValue.put(ReactorKeysEnum.END_DATE.getKey(), "2026-08-31");

		try (MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SecurityModelMetadataUtils> metadataUtils = Mockito
						.mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "model-alias"))
					.thenReturn("model-id");
			engineUtils.when(() -> SecurityEngineUtils.userCanViewEngine(user, "model-id")).thenReturn(true);
			engineUtils.when(() -> SecurityEngineUtils.getEngineType("model-id"))
					.thenReturn(IEngine.CATALOG_TYPE.MODEL);
			engineUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMapForUserId("current-user", "model-id"))
					.thenReturn(List.of(permission));
			metadataUtils.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id")).thenReturn(Map.of());
			utility.when(Utility::isModelInferenceLogsEnabled).thenReturn(true);
			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(eq("credit"),
					eq("current-user"), eq("model-id"), any(ZonedDateTime.class), any(ZonedDateTime.class)))
					.thenReturn(3D);

			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) reactor.execute().getValue();

			assertEquals("CUSTOM", result.get("rangeType"));
			assertEquals(3D, result.get("creditsUsed"));
			assertNull(result.get("creditsRemaining"));
			assertNull(result.get("limitExceeded"));
			assertEquals("2026-08-01T00:00Z", result.get("periodStart"));
			assertEquals("2026-08-31T23:59:59.999999999Z", result.get("periodEnd"));
		}
	}

	@Test
	void rejectsInaccessibleModel() {
		try (MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class)) {
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "model-alias"))
					.thenReturn("model-id");
			engineUtils.when(() -> SecurityEngineUtils.userCanViewEngine(user, "model-id")).thenReturn(false);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertTrue(exception.getMessage().contains("does not have access"));
		}
	}

	@Test
	void rejectsAnotherUserForNonAdmin() {
		reactor.keyValue.put("userId", "other-user");

		try (MockedStatic<SecurityAdminUtils> adminUtils = Mockito.mockStatic(SecurityAdminUtils.class)) {
			adminUtils.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertTrue(exception.getMessage().contains("must be an admin"));
		}
	}

	@Test
	void allowsAdminToViewAnotherUser() {
		reactor.keyValue.put("userId", "other-user");

		try (MockedStatic<SecurityAdminUtils> adminUtils = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityQueryUtils> queryUtils = Mockito.mockStatic(SecurityQueryUtils.class);
				MockedStatic<SecurityEngineUtils> engineUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SecurityModelMetadataUtils> metadataUtils = Mockito
						.mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			adminUtils.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
			queryUtils.when(() -> SecurityQueryUtils.checkUserExist("other-user")).thenReturn(true);
			queryUtils.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "model-alias"))
					.thenReturn("model-id");
			engineUtils.when(() -> SecurityEngineUtils.getEngineType("model-id"))
					.thenReturn(IEngine.CATALOG_TYPE.MODEL);
			engineUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMapForUserId("other-user", "model-id"))
					.thenReturn(List.of());
			metadataUtils.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id"))
					.thenReturn(Map.of());
			utility.when(Utility::isModelInferenceLogsEnabled).thenReturn(false);

			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) reactor.execute().getValue();

			assertEquals("other-user", result.get("userId"));
			assertFalse((Boolean) result.get("restrictionEnabled"));
		}
	}
}
