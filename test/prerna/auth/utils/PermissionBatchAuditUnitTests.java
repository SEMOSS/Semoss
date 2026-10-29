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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionSubject.SubjectCategory;
import prerna.auth.access.PermissionTargetResource;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Utility;

class PermissionBatchAuditUnitTests {
    private PermissionBatchValidation batch(PermissionTargetResource target, PermissionChange.Action action,
            PermissionSubject subject) {
        PermissionChange change = new PermissionChange(action, subject,
                action == PermissionChange.Action.REMOVE ? null : new PermissionGrant("EDIT", null));
        PermissionBatchValidation batch = new PermissionBatchValidation(new PermissionBatch(target, List.of(change)));
        PermissionChangeValidation validation = batch.getChanges().get(0);
        validation.setSubjectId(subject.getId());
        validation.setSubjectType(subject.getType());
        validation.setCurrentLevelId(action == PermissionChange.Action.ADD ? null : 3);
        validation.setLevelId(action == PermissionChange.Action.REMOVE ? null : 2);
        return batch;
    }

    private JsonObject json(Object event) {
        return ProjectPermissionAuditUnitTests.AUDIT_JSON.toJsonTree(event).getAsJsonObject();
    }

    private static ModelUsageLimit tokensADay(long amount) {
        return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.EACH_PERSON)));
    }

    /** Makes the batch's change write a usage limit, as validation does when it differs from the current one. */
    private static void changesLimit(PermissionBatchValidation batch, ModelUsageLimitKey key, ModelUsageLimit before,
            ModelUsageLimit after) {
        PermissionChangeValidation validation = batch.getChanges().get(0);
        validation.setCurrentUsageLimit(before);
        validation.setUsageLimit(after);
        validation.setLimitKey(key);
    }

    private List<JsonObject> record(PermissionBatchValidation batch, PermissionBatchOptions options) {
        List<JsonObject> events = new ArrayList<>();
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
                events.add(json(call.getArgument(0)));
                return null;
            });
            PermissionBatchAudit.recordChanges(null, List.of(batch), options);
        }
        return events;
    }

    private static List<String> types(List<JsonObject> events) {
        return events.stream().map(event -> event.get("eventType").getAsString()).toList();
    }

    /** The first rule's amount of the limit in an event's old or new value, or null for no limit defined. */
    private static Long amount(JsonObject event, String value) {
        JsonObject values = event.getAsJsonObject(value);
        if (values == null || !values.has("usageLimit") || values.get("usageLimit").isJsonNull()) {
            return null;
        }
        return values.getAsJsonObject("usageLimit").getAsJsonArray("rules").get(0).getAsJsonObject().get("amount")
                .getAsLong();
    }

    @ParameterizedTest
    @EnumSource(PermissionChange.Action.class)
    void mapsDirectChangesToSingleEventsWithCanonicalScope(PermissionChange.Action action) {
        PermissionBatchValidation batch = batch(PermissionTargetResource.insight("project", "insight"), action,
                PermissionSubject.ofUser("member", "NATIVE"));
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
                JsonObject event = json(call.getArgument(0));
                assertEquals(switch (action) {
                    case ADD -> "PERMISSION_ADD";
                    case CHANGE -> "PERMISSION_UPDATE";
                    case REMOVE -> "PERMISSION_DELETE";
                }, event.get("eventType").getAsString());
                assertEquals("project", event.get("projectId").getAsString());
                assertEquals("insight", event.get("insightId").getAsString());
                assertEquals("member", event.get("subjectUserId").getAsString());
                assertEquals("NATIVE", event.get("subjectUserType").getAsString());
                assertEquals(action != PermissionChange.Action.ADD, event.has("oldValueRaw"));
                assertEquals(action != PermissionChange.Action.REMOVE, event.has("newValueRaw"));
                return null;
            });
            PermissionBatchAudit.recordChanges(null, List.of(batch), PermissionBatchOptions.userMode());
            audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
        }
    }

    @Test
    void groupGrantDoesNotMisidentifyTheGroupAsAnAffectedUser() {
        PermissionBatchValidation batch = batch(PermissionTargetResource.engine("engine"), PermissionChange.Action.ADD,
                PermissionSubject.ofTeam("team", "CUSTOM"));
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
                JsonObject event = json(call.getArgument(0));
                assertFalse(event.has("subjectUserId"));
                assertEquals("team", event.getAsJsonObject("detailsRaw").get("groupId").getAsString());
                assertEquals("engine", event.get("engineId").getAsString());
                return null;
            });
            PermissionBatchAudit.recordChanges(null, List.of(batch), PermissionBatchOptions.adminMode());
            audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
        }
    }

    @ParameterizedTest
    @EnumSource(value = PermissionChange.Action.class, names = { "ADD", "REMOVE" })
    void auditsManagerChangesAsGroupEvents(PermissionChange.Action action) {
        PermissionBatchValidation batch = batch(PermissionTargetResource.teamManagers("team"), action,
                PermissionSubject.ofUser("member", "NATIVE"));
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
                JsonObject event = json(call.getArgument(0));
                assertEquals("GROUP_MANAGER_" + action.name(), event.get("eventType").getAsString());
                assertEquals("GROUP", event.get("targetType").getAsString());
                assertEquals("team", event.get("targetId").getAsString());
                assertEquals("member", event.get("subjectUserId").getAsString());
                assertEquals("NATIVE", event.get("subjectUserType").getAsString());
                assertFalse(event.has("oldValueRaw"));
                assertFalse(event.has("newValueRaw"));
                return null;
            });
            PermissionBatchAudit.recordChanges(null, List.of(batch), PermissionBatchOptions.userMode());
            audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
        }
    }

    @Test
    void approvalRetainsProviderAndExplicitApproverWithoutDuplicatingGrant() {
        PermissionTargetResource target = PermissionTargetResource.project("project");
        PermissionBatchValidation batch = batch(target, PermissionChange.Action.ADD,
                PermissionSubject.ofUser("member", null));
        PermissionBatchOptions options = PermissionBatchOptions.adminMode().grantedBy("admin", "NATIVE")
                .approving(target, List.of(Map.of("userid", "member", "type", "NATIVE",
                        "permission", "EDIT", "requestid", "request")));
        List<JsonObject> events = new ArrayList<>();
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
                events.add(json(call.getArgument(0)));
                return null;
            });
            PermissionBatchAudit.recordChanges(null, List.of(batch), options);
            assertEquals(List.of("PERMISSION_ADD", "ACCESS_REQUEST_APPROVE"),
                    events.stream().map(event -> event.get("eventType").getAsString()).toList());
            for (JsonObject event : events) {
                assertEquals("NATIVE", event.get("subjectUserType").getAsString());
                assertEquals("admin", event.get("actorUserId").getAsString());
            }
        }
    }

    @Test
    void summariesSuppressPerRowEventsAndDisabledTrackingDoesNothing() {
        PermissionBatchValidation batch = batch(PermissionTargetResource.project("project"), PermissionChange.Action.ADD,
                PermissionSubject.ofUser("member", "NATIVE"));
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            PermissionBatchAudit.recordChanges(null, List.of(batch), PermissionBatchOptions.adminMode().withSummaryAudit());
            utility.when(Utility::isUserTrackingEnabled).thenReturn(false);
            PermissionBatchAudit.recordChanges(null, List.of(batch), PermissionBatchOptions.userMode());
            PermissionBatchAudit.recordDenials(null, PermissionTargetResource.project("project"), List.of("request"),
                    PermissionBatchOptions.userMode());
            audit.verifyNoInteractions();
        }
    }

    @Test
    void aGrantChangeThatChangesItsLimitRecordsTheLimitBesideTheAccess() {
        PermissionBatchValidation batch = batch(PermissionTargetResource.engine("model"), PermissionChange.Action.CHANGE,
                PermissionSubject.ofUser("member", "NATIVE"));
        changesLimit(batch, ModelUsageLimitKey.direct("model", "member", "NATIVE"), tokensADay(500), tokensADay(700));

        List<JsonObject> events = record(batch, PermissionBatchOptions.userMode());

        assertEquals(List.of("PERMISSION_UPDATE", "USAGE_LIMIT_UPDATE"), types(events));
        JsonObject limit = events.get(1);
        assertEquals("model", limit.get("engineId").getAsString());
        assertEquals("member", limit.get("subjectUserId").getAsString());
        assertEquals("DIRECT", limit.getAsJsonObject("detailsRaw").get("limitType").getAsString());
        assertEquals(500L, amount(limit, "oldValueRaw"));
        assertEquals(700L, amount(limit, "newValueRaw"));
    }

    @Test
    void aGrantChangeThatKeepsItsLimitRecordsOnlyTheAccess() {
        PermissionBatchValidation batch = batch(PermissionTargetResource.engine("model"), PermissionChange.Action.CHANGE,
                PermissionSubject.ofUser("member", "NATIVE"));
        batch.getChanges().get(0).keepUsageLimit(tokensADay(500));

        assertEquals(List.of("PERMISSION_UPDATE"), types(record(batch, PermissionBatchOptions.userMode())));
    }

    @Test
    void aTeamLimitNamesTheTeamAndNoAffectedUser() {
        PermissionBatchValidation batch = batch(PermissionTargetResource.engine("model"), PermissionChange.Action.ADD,
                PermissionSubject.ofTeam("ops", "CUSTOM"));
        changesLimit(batch, ModelUsageLimitKey.team("model", "ops", "CUSTOM"), null, tokensADay(1_000));

        JsonObject limit = record(batch, PermissionBatchOptions.userMode()).get(1);

        assertEquals("USAGE_LIMIT_UPDATE", limit.get("eventType").getAsString());
        assertFalse(limit.has("subjectUserId"));
        JsonObject details = limit.getAsJsonObject("detailsRaw");
        assertEquals("TEAM", details.get("limitType").getAsString());
        assertEquals("ops", details.get("groupId").getAsString());
        assertNull(amount(limit, "oldValueRaw"));
        assertEquals(1_000L, amount(limit, "newValueRaw"));
    }

    @Test
    void thePublicUseLimitAndTheModelTotalAreRecordedAsLimitChangesOnly() {
        PermissionBatchValidation publicUse = batch(PermissionTargetResource.engine("model"),
                PermissionChange.Action.CHANGE, new PermissionSubject(SubjectCategory.PUBLIC, null, null));
        changesLimit(publicUse, ModelUsageLimitKey.publicUse("model"), null, tokensADay(50_000));
        PermissionBatchValidation total = batch(PermissionTargetResource.engine("model"),
                PermissionChange.Action.CHANGE, new PermissionSubject(SubjectCategory.MODEL, null, null));
        changesLimit(total, ModelUsageLimitKey.modelTotal("model"), tokensADay(9_000_000), null);

        List<JsonObject> publicEvents = record(publicUse, PermissionBatchOptions.userMode());
        List<JsonObject> totalEvents = record(total, PermissionBatchOptions.userMode());

        assertEquals(List.of("USAGE_LIMIT_UPDATE"), types(publicEvents));
        assertEquals("PUBLIC", publicEvents.get(0).getAsJsonObject("detailsRaw").get("limitType").getAsString());
        assertEquals(List.of("USAGE_LIMIT_UPDATE"), types(totalEvents));
        assertEquals("MODEL_TOTAL", totalEvents.get(0).getAsJsonObject("detailsRaw").get("limitType").getAsString());
        assertEquals(9_000_000L, amount(totalEvents.get(0), "oldValueRaw"));
        assertNull(amount(totalEvents.get(0), "newValueRaw"));
    }

    @Test
    void aRemovedGrantRecordsItsLimitRemovedWithItEvenUnderASummary() {
        PermissionBatchValidation batch = batch(PermissionTargetResource.engine("model"), PermissionChange.Action.REMOVE,
                PermissionSubject.ofUser("member", "NATIVE"));
        changesLimit(batch, ModelUsageLimitKey.direct("model", "member", "NATIVE"), tokensADay(500), null);

        List<JsonObject> events = record(batch, PermissionBatchOptions.adminMode().withSummaryAudit());

        // the summary replaces the access event, but each limit change is still recorded
        assertEquals(List.of("USAGE_LIMIT_UPDATE"), types(events));
        JsonObject details = events.get(0).getAsJsonObject("detailsRaw");
        assertTrue(details.get("removedWithGrant").getAsBoolean());
        assertEquals(500L, amount(events.get(0), "oldValueRaw"));
    }

    @Test
    void rejectionIsScopedAndAuditFailuresDoNotFailCommittedChanges() {
        try (var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
            utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
            audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
                JsonObject event = json(call.getArgument(0));
                assertEquals("ACCESS_REQUEST_REJECT", event.get("eventType").getAsString());
                assertEquals("engine", event.get("engineId").getAsString());
                assertEquals("request", event.getAsJsonObject("detailsRaw").get("requestId").getAsString());
                throw new IllegalStateException("audit unavailable");
            });
            assertDoesNotThrow(() -> PermissionBatchAudit.recordDenials(null, PermissionTargetResource.engine("engine"),
                    List.of("request"), PermissionBatchOptions.adminMode()));
            audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
        }
    }
}
