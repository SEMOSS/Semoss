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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.DayOfWeek;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

public class ModelUsageRestrictionUtilityUnitTests extends SemossUnitTest {
	User user;
	AuthProvider auth;
	AccessToken access;

	@BeforeEach
	void setup() throws Exception {
		FileUtils.cleanDirectory(tempDir.toFile());

		user = mock(User.class);
		auth = mock(AuthProvider.class);
		access = mock(AccessToken.class);
	}

	@AfterEach
	void tearDown() throws Exception {
		// Clean up if needed
	}

	@Test
	void getModelUsageRestriction_NoPermissions() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class)) {
			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(null);

			Map<String, Object> result = ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId");

			assertNotNull(result);
			assertTrue(result.isEmpty());
		}
	}

	@Test
	void getModelUsageRestriction_EmptyPermissions() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class)) {
			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(new ArrayList<>());

			Map<String, Object> result = ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId");

			assertNotNull(result);
			assertTrue(result.isEmpty());
		}
	}

	@Test
	void getModelUsageRestriction_EngineTokenRestriction_WithinLimit() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "token");
			permissionMap.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "DAY");
			permissionMap.put(Constants.ENGINE_MAX_TOKEN_KEY, 10000);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(true);
			utilityStatic.when(() -> Utility.getCurrentZonedDateTimeUTC())
					.thenReturn(ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC));

			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					eq("token"), eq(user), eq("engineId"), any(ZonedDateTime.class), eq("DAY")))
					.thenReturn(5000);

			Map<String, Object> result = ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId");

			assertNotNull(result);
			assertEquals("token", result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE));
			assertEquals(5000, result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE));
			assertEquals(10000, result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE));
		}
	}

	@Test
	void getModelUsageRestriction_EngineTokenRestriction_ExceedsLimit() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "token");
			permissionMap.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "MONTH");
			permissionMap.put(Constants.ENGINE_MAX_TOKEN_KEY, 10000);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(true);
			utilityStatic.when(() -> Utility.getCurrentZonedDateTimeUTC())
					.thenReturn(ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC));

			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					eq("token"), eq(user), eq("engineId"), any(ZonedDateTime.class), eq("MONTH")))
					.thenReturn(15000);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId"));

			assertTrue(e.getMessage().contains("15000"));
			assertTrue(e.getMessage().contains("10000"));
		}
	}

	@Test
	void getModelUsageRestriction_EngineComputeRestriction_WithinLimit() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "compute");
			permissionMap.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "WEEK");
			permissionMap.put(Constants.ENGINE_MAX_RESPONSE_TIME_KEY, 50000.0);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(true);
			utilityStatic.when(() -> Utility.getCurrentZonedDateTimeUTC())
					.thenReturn(ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC));

			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					eq("compute"), eq(user), eq("engineId"), any(ZonedDateTime.class), eq("WEEK")))
					.thenReturn(25000.0);

			Map<String, Object> result = ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId");

			assertNotNull(result);
			assertEquals("compute", result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE));
			assertEquals(25000, result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE));
			assertEquals(50000, result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE));
		}
	}

	@Test
	void getModelUsageRestriction_EngineComputeRestriction_ExceedsLimit() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "compute");
			permissionMap.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "YEAR");
			permissionMap.put(Constants.ENGINE_MAX_RESPONSE_TIME_KEY, 100000.0);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(true);
			utilityStatic.when(() -> Utility.getCurrentZonedDateTimeUTC())
					.thenReturn(ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC));

			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					eq("compute"), eq(user), eq("engineId"), any(ZonedDateTime.class), eq("YEAR")))
					.thenReturn(150000.0);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId"));

			assertTrue(e.getMessage().contains("150000"));
			assertTrue(e.getMessage().contains("100000"));
		}
	}

	@Test
	void getModelUsageRestriction_UserTokenRestriction_WithinLimit() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.USER_USAGE_RESTRICTION_KEY, "token");
			permissionMap.put(Constants.USER_MODEL_USAGE_FREQUENCY_KEY, "MONTH");
			permissionMap.put(Constants.USER_MODEL_MAX_TOKEN_KEY, 20000);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(true);
			utilityStatic.when(() -> Utility.getCurrentZonedDateTimeUTC())
					.thenReturn(ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC));

			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalUsageForUser(
					eq("token"), eq(user), eq("engineId"), any(ZonedDateTime.class), eq("MONTH")))
					.thenReturn(8000);

			Map<String, Object> result = ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId");

			assertNotNull(result);
			assertEquals("token", result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE));
			assertEquals(8000, result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE));
			assertEquals(20000, result.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE));
		}
	}

	@Test
	void getModelUsageRestriction_UserTokenRestriction_ExceedsLimit() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class);
				MockedStatic<ModelInferenceLogsUtils> logsUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.USER_USAGE_RESTRICTION_KEY, "token");
			permissionMap.put(Constants.USER_MODEL_USAGE_FREQUENCY_KEY, "ALL_TIME");
			permissionMap.put(Constants.USER_MODEL_MAX_TOKEN_KEY, 5000);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(true);
			utilityStatic.when(() -> Utility.getCurrentZonedDateTimeUTC())
					.thenReturn(ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC));

			logsUtils.when(() -> ModelInferenceLogsUtils.getTotalUsageForUser(
					eq("token"), eq(user), eq("engineId"), any(ZonedDateTime.class), eq("ALL_TIME")))
					.thenReturn(7500);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId"));

			assertTrue(e.getMessage().contains("7500"));
			assertTrue(e.getMessage().contains("5000"));
		}
	}

	@Test
	void getModelUsageRestriction_InferenceLogsNotEnabled() {
		try (MockedStatic<SecurityEngineUtils> securityUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utilityStatic = Mockito.mockStatic(Utility.class)) {

			Map<String, Object> permissionMap = new HashMap<>();
			permissionMap.put(Constants.ENGINE_USAGE_RESTRICTION_KEY, "token");
			permissionMap.put(Constants.ENGINE_USAGE_FREQUENCY_KEY, "DAY");
			permissionMap.put(Constants.ENGINE_MAX_TOKEN_KEY, 10000);

			List<Map<String, Object>> permissions = new ArrayList<>();
			permissions.add(permissionMap);

			securityUtils.when(() -> SecurityEngineUtils.getEngineUsagePermissionMap(user, "engineId"))
					.thenReturn(permissions);
			utilityStatic.when(() -> Utility.isModelInferenceLogsEnabled()).thenReturn(false);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> ModelUsageRestrictionUtility.getModelUsageRestriction(user, "engineId"));

			assertTrue(e.getMessage().contains("not properly configured"));
		}
	}

	@Test
	void selectEffectiveRowsByFrequency_SpecificTeamOverridesDefaultTeam() {
		Map<String, Map<String, Object>> selected = ModelUsageRestrictionUtility.selectEffectiveRowsByFrequency(
				List.of(), List.of(), List.of(limit("DAY", 100, true)), List.of(limit("DAY", 500, true)));

		assertEquals(100, selected.get("DAY").get("maxTokens"));
	}

	@Test
	void selectEffectiveRowsByFrequency_UsesHighestSpecificTeamLimit() {
		Map<String, Map<String, Object>> selected = ModelUsageRestrictionUtility.selectEffectiveRowsByFrequency(
				List.of(), List.of(), List.of(limit("DAY", 100, true), limit("DAY", 300, true)), List.of());

		assertEquals(300, selected.get("DAY").get("maxTokens"));
	}

	@Test
	void selectEffectiveRowsByFrequency_UsesHigherSpecificUserOrTeamLimit() {
		Map<String, Map<String, Object>> selected = ModelUsageRestrictionUtility.selectEffectiveRowsByFrequency(
				List.of(limit("DAY", 200, false)), List.of(), List.of(limit("DAY", 300, true)), List.of());

		assertEquals(300, selected.get("DAY").get("maxTokens"));
	}

	@Test
	void selectEffectiveRowsByFrequency_SpecificUserOverridesDefaultUser() {
		Map<String, Map<String, Object>> selected = ModelUsageRestrictionUtility.selectEffectiveRowsByFrequency(
				List.of(limit("DAY", 200, false)), List.of(limit("DAY", 500, false)), List.of(), List.of());

		assertEquals(200, selected.get("DAY").get("maxTokens"));
	}

	@Test
	void selectEffectiveRowsByFrequency_UsesHigherDefaultUserOrTeamLimit() {
		Map<String, Map<String, Object>> selected = ModelUsageRestrictionUtility.selectEffectiveRowsByFrequency(
				List.of(), List.of(limit("DAY", 200, false)), List.of(), List.of(limit("DAY", 300, true)));

		assertEquals(300, selected.get("DAY").get("maxTokens"));
	}

	private static Map<String, Object> limit(String frequency, int maxTokens, boolean teamScoped) {
		Map<String, Object> limit = new HashMap<>();
		limit.put("usageRestriction", Constants.MODEL_TOKEN_RESTRICTION_VALUE);
		limit.put("usageFrequency", frequency);
		limit.put("maxTokens", maxTokens);
		limit.put("_teamScoped", teamScoped);
		return limit;
	}

	@Test
	void updateRestrictionMapCurrentUsage_TokenRestriction() {
		@SuppressWarnings("unchecked")
		AbstractModelEngineResponse<String> mockResponse = mock(AbstractModelEngineResponse.class);

		Map<String, Object> restrictionMap = new HashMap<>();
		restrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, "token");
		restrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, 1000);
		restrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, 10000);

		when(mockResponse.getNumberOfTokensInPrompt()).thenReturn(50);
		when(mockResponse.getNumberOfTokensInResponse()).thenReturn(100);

		ZonedDateTime inputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC);
		ZonedDateTime outputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 5, 0, ZoneOffset.UTC);

		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(restrictionMap, mockResponse, inputTime, outputTime);

		assertEquals(1150, restrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE));
		verify(mockResponse, times(1)).setUsageRestriction(restrictionMap);
	}

	@Test
	void updateRestrictionMapCurrentUsage_ComputeRestriction() {
		@SuppressWarnings("unchecked")
		AbstractModelEngineResponse<String> mockResponse = mock(AbstractModelEngineResponse.class);

		Map<String, Object> restrictionMap = new HashMap<>();
		restrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, "compute");
		restrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, 5000.0);
		restrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, 50000.0);

		ZonedDateTime inputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC);
		ZonedDateTime outputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 3, 500000000, ZoneOffset.UTC); // 3.5 seconds

		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(restrictionMap, mockResponse, inputTime, outputTime);

		assertEquals(8500.0, restrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE));
		verify(mockResponse, times(1)).setUsageRestriction(restrictionMap);
	}

	@Test
	void updateRestrictionMapCurrentUsage_NullMap() {
		@SuppressWarnings("unchecked")
		AbstractModelEngineResponse<String> mockResponse = mock(AbstractModelEngineResponse.class);

		ZonedDateTime inputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC);
		ZonedDateTime outputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 5, 0, ZoneOffset.UTC);

		// Should not throw exception or call setUsageRestriction
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(null, mockResponse, inputTime, outputTime);

		verify(mockResponse, times(0)).setUsageRestriction(any());
	}

	@Test
	void updateRestrictionMapCurrentUsage_EmptyMap() {
		@SuppressWarnings("unchecked")
		AbstractModelEngineResponse<String> mockResponse = mock(AbstractModelEngineResponse.class);

		Map<String, Object> emptyMap = new HashMap<>();

		ZonedDateTime inputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC);
		ZonedDateTime outputTime = ZonedDateTime.of(2025, 1, 15, 12, 0, 5, 0, ZoneOffset.UTC);

		// Should not throw exception or call setUsageRestriction
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(emptyMap, mockResponse, inputTime, outputTime);

		verify(mockResponse, times(0)).setUsageRestriction(any());
	}

	// ========== Date Range Tests ==========

	@Test
	void getDateRangeFromFrequency_DAY() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 45, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("DAY", testDate);

		assertNotNull(result);
		assertEquals(ZonedDateTime.of(2025, 1, 15, 0, 0, 0, 0, ZoneOffset.UTC), result.get("start"));
		assertEquals(ZonedDateTime.of(2025, 1, 15, 23, 59, 59, 999999999, ZoneOffset.UTC), result.get("end"));
	}

	@Test
	void getDateRangeFromFrequency_WEEK() {
		// Wednesday, January 15, 2025
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("WEEK", testDate);

		assertNotNull(result);
		// Week should start on Sunday, Jan 12
		assertEquals(DayOfWeek.SUNDAY, result.get("start").getDayOfWeek());
		assertEquals(12, result.get("start").getDayOfMonth());
		// Week should end on Saturday, Jan 18
		assertEquals(DayOfWeek.SATURDAY, result.get("end").getDayOfWeek());
		assertEquals(18, result.get("end").getDayOfMonth());
	}

	@Test
	void getDateRangeFromFrequency_MONTH() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("MONTH", testDate);

		assertNotNull(result);
		assertEquals(1, result.get("start").getDayOfMonth());
		assertEquals(1, result.get("start").getMonthValue());
		assertEquals(31, result.get("end").getDayOfMonth());
		assertEquals(1, result.get("end").getMonthValue());
	}

	@Test
	void getDateRangeFromFrequency_YEAR() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 6, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("YEAR", testDate);

		assertNotNull(result);
		assertEquals(1, result.get("start").getDayOfYear());
		assertEquals(2025, result.get("start").getYear());
		assertEquals(365, result.get("end").getDayOfYear()); // 2025 is not a leap year
		assertEquals(2025, result.get("end").getYear());
	}

	@Test
	void getDateRangeFromFrequency_ALL_TIME() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("ALL_TIME", testDate);

		assertNotNull(result);
		assertEquals(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC), result.get("start"));
		assertEquals(testDate, result.get("end"));
	}

	@Test
	void getDateRangeFromFrequency_NullFrequency() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency(null, testDate);

		assertNotNull(result);
		// Should default to DAY
		assertEquals(ZonedDateTime.of(2025, 1, 15, 0, 0, 0, 0, ZoneOffset.UTC), result.get("start"));
		assertEquals(ZonedDateTime.of(2025, 1, 15, 23, 59, 59, 999999999, ZoneOffset.UTC), result.get("end"));
	}

	@Test
	void getDateRangeFromFrequency_EmptyFrequency() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("", testDate);

		assertNotNull(result);
		// Should default to DAY
		assertEquals(ZonedDateTime.of(2025, 1, 15, 0, 0, 0, 0, ZoneOffset.UTC), result.get("start"));
		assertEquals(ZonedDateTime.of(2025, 1, 15, 23, 59, 59, 999999999, ZoneOffset.UTC), result.get("end"));
	}

	@Test
	void getDateRangeFromFrequency_InvalidFrequency() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result = ModelUsageRestrictionUtility.getDateRangeFromFrequency("INVALID", testDate);

		assertNotNull(result);
		// Should default to DAY
		assertEquals(ZonedDateTime.of(2025, 1, 15, 0, 0, 0, 0, ZoneOffset.UTC), result.get("start"));
		assertEquals(ZonedDateTime.of(2025, 1, 15, 23, 59, 59, 999999999, ZoneOffset.UTC), result.get("end"));
	}

	@Test
	void getDateRangeFromFrequency_CaseInsensitive() {
		ZonedDateTime testDate = ZonedDateTime.of(2025, 1, 15, 14, 30, 0, 0, ZoneOffset.UTC);

		Map<String, ZonedDateTime> result1 = ModelUsageRestrictionUtility.getDateRangeFromFrequency("week", testDate);
		Map<String, ZonedDateTime> result2 = ModelUsageRestrictionUtility.getDateRangeFromFrequency("WEEK", testDate);
		Map<String, ZonedDateTime> result3 = ModelUsageRestrictionUtility.getDateRangeFromFrequency("Week", testDate);

		assertEquals(result1.get("start"), result2.get("start"));
		assertEquals(result1.get("start"), result3.get("start"));
		assertEquals(result1.get("end"), result2.get("end"));
		assertEquals(result1.get("end"), result3.get("end"));
	}

}
