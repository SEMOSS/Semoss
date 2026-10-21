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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Utility;

/**
 * Audit events for the usage limits admins set across models, in
 * {@link ModelUsageLimitAudit}.
 */
class ModelUsageLimitAuditUnitTests {

	private static ModelUsageLimit costADay(long micros) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.COST, micros, Window.DAY, Counts.EACH_PERSON)));
	}

	private static List<JsonObject> record(ModelUsageLimitKey key, ModelUsageLimit before, ModelUsageLimit after) {
		List<JsonObject> events = new ArrayList<>();
		try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
				events.add(ProjectPermissionAuditUnitTests.AUDIT_JSON.toJsonTree(call.getArgument(0)).getAsJsonObject());
				return null;
			});
			ModelUsageLimitAudit.recordAdminChange(null, key, before, after);
		}
		return events;
	}

	@Test
	void namesEachLimitByThePathsLimitType() {
		assertEquals("DIRECT", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.direct("m", "jdoe", "NATIVE")));
		assertEquals("ALL_MODELS", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.allModels("jdoe", "NATIVE")));
		assertEquals("TEAM", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.team("m", "ops", "CUSTOM")));
		assertEquals("TEAM_TOTAL", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.teamTotal("ops", "CUSTOM")));
		assertEquals("PUBLIC", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.publicUse("m")));
		assertEquals("MODEL_TOTAL", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.modelTotal("m")));
		assertEquals("PLATFORM", ModelUsageLimitAudit.limitTypeOf(ModelUsageLimitKey.platform()));
	}

	@Test
	void aUsersTotalModelLimitIsRecordedAgainstTheUser() {
		List<JsonObject> events = record(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), null, costADay(5_000_000));

		assertEquals(1, events.size());
		JsonObject event = events.get(0);
		assertEquals("USAGE_LIMIT_UPDATE", event.get("eventType").getAsString());
		assertEquals("USER", event.get("targetType").getAsString());
		assertEquals("jdoe", event.get("subjectUserId").getAsString());
		assertEquals("ALL_MODELS", event.getAsJsonObject("detailsRaw").get("limitType").getAsString());
	}

	@Test
	void theGeneralPlatformLimitIsRecordedAsHighSeverity() {
		JsonObject event = record(ModelUsageLimitKey.platform(), costADay(1_000_000), costADay(2_000_000)).get(0);

		assertEquals("PLATFORM", event.get("targetType").getAsString());
		assertEquals("HIGH", event.get("severity").getAsString());
	}

	@Test
	void aTeamTotalNamesTheTeam() {
		JsonObject event = record(ModelUsageLimitKey.teamTotal("ops", "CUSTOM"), costADay(1_000_000), null).get(0);

		assertEquals("GROUP", event.get("targetType").getAsString());
		assertEquals("ops", event.get("targetId").getAsString());
		assertEquals("TEAM_TOTAL", event.getAsJsonObject("detailsRaw").get("limitType").getAsString());
		assertFalse(event.has("subjectUserId"));
	}

	@Test
	void anUnchangedLimitRecordsNothingAndAFailureDoesNotThrow() {
		assertEquals(List.of(), record(ModelUsageLimitKey.platform(), costADay(1_000_000), costADay(1_000_000)));
		try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenThrow(new IllegalStateException("down"));
			assertDoesNotThrow(
					() -> ModelUsageLimitAudit.recordAdminChange(null, ModelUsageLimitKey.platform(), null, costADay(1)));
		}
	}

	@Test
	void theLimitsOfAnExpiredGrantAreRecordedAsRemovedBySemoss() {
		List<JsonObject> events = new ArrayList<>();
		try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
				events.add(ProjectPermissionAuditUnitTests.AUDIT_JSON.toJsonTree(call.getArgument(0)).getAsJsonObject());
				return null;
			});
			ModelUsageLimitAudit.recordRemovedWithExpiredGrant("m",
					Map.of(ModelUsageLimitKey.direct("m", "jdoe", "NATIVE"), costADay(1_000_000)));
		}

		assertEquals(1, events.size());
		JsonObject event = events.get(0);
		assertEquals("SYSTEM", event.get("actorUserType").getAsString());
		assertEquals("jdoe", event.get("subjectUserId").getAsString());
		assertEquals("m", event.get("engineId").getAsString());
		JsonObject details = event.getAsJsonObject("detailsRaw");
		assertEquals("EXPIRED", details.get("reason").getAsString());
		assertEquals("DIRECT", details.get("limitType").getAsString());
	}
}
