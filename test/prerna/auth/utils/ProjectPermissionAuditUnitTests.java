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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

class ProjectPermissionAuditUnitTests extends AbstractSecurityUtilsUnitTestsSetup {
	static final String PROJECT = "auditproject";
	static final String MEMBER = "auditmemberid";
	static final Gson AUDIT_JSON = new GsonBuilder().addSerializationExclusionStrategy(new ExclusionStrategy() {
		@Override
		public boolean shouldSkipField(FieldAttributes field) {
			return field.getName().equals("actorUser");
		}

		@Override
		public boolean shouldSkipClass(Class<?> type) {
			return false;
		}
	}).create();

	IRDBMSEngine securityDb;
	User actor;
	List<String> tables = new ArrayList<>();
	boolean hasFailureConstraint;

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		actor = UnitTestSecurityAuthUtils.createUser("auditactor", true);
		UnitTestSecurityAuthUtils.createUser("auditmember", false);
		UnitTestSecurityAuthUtils.createProject(PROJECT, "Audit project", actor);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(actor, PROJECT, MEMBER, "READ_ONLY");
	}

	@AfterEach
	void cleanup() throws Exception {
		if (hasFailureConstraint) {
			QueryExecutionUtility.executeUpdate(securityDb,
					"ALTER TABLE PROJECTPERMISSION DROP CONSTRAINT AUDIT_FAIL_UPDATE",
					QueryExecutionUtility.NO_PARAMETERS);
		}
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void bulkUpdateAuditsOnlyAfterSuccessfulCommit(boolean succeeds) throws Exception {
		List<Map<String, String>> requests = List.of(Map.of("userid", MEMBER, "type", "NATIVE", "permission", "EDIT"));
		if (!succeeds) {
			QueryExecutionUtility.executeUpdate(securityDb,
					"ALTER TABLE PROJECTPERMISSION ADD CONSTRAINT AUDIT_FAIL_UPDATE CHECK (PERMISSION<>2)",
					QueryExecutionUtility.NO_PARAMETERS);
			hasFailureConstraint = true;
		}
		try (var utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(invocation -> {
				JsonObject event = AUDIT_JSON.toJsonTree(invocation.getArgument(0)).getAsJsonObject();
				assertEquals("PERMISSION_UPDATE", event.get("eventType").getAsString());
				assertEquals(PROJECT, event.get("projectId").getAsString());
				assertEquals(MEMBER, event.get("subjectUserId").getAsString());
				assertEquals("READ_ONLY", event.getAsJsonObject("oldValueRaw").get("permission").getAsString());
				assertEquals("EDIT", event.getAsJsonObject("newValueRaw").get("permission").getAsString());
				// A separate pooled read sees the committed permission, not an in-flight write.
				assertEquals(2, SecurityProjectUtils.getUserProjectPermission(MEMBER, PROJECT));
				return null;
			});
			if (succeeds) {
				SecurityProjectUtils.editProjectUserPermissions(actor, PROJECT, requests, null);
				audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
			} else {
				assertThrows(IllegalArgumentException.class,
						() -> SecurityProjectUtils.editProjectUserPermissions(actor, PROJECT, requests, null));
				assertEquals(3, SecurityProjectUtils.getUserProjectPermission(MEMBER, PROJECT));
				audit.verifyNoInteractions();
			}
		}
	}

	@Test
	void repeatedGrantAllDoesNotWriteOrAuditANoOp() {
		UnitTestSecurityAuthUtils.createEngine("auditengine", "Audit engine", actor);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(actor, MEMBER, "auditengine", "READ_ONLY");
		SecurityAdminUtils admin = SecurityAdminUtils.getInstance(actor);
		try (var utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			assertDoesNotThrow(() -> admin.grantAllProjects(MEMBER, "EDIT", true, actor));
			assertDoesNotThrow(() -> admin.grantAllEngines(MEMBER, "EDIT", true, null, actor));
			audit.verifyNoInteractions();
			assertEquals(3, SecurityProjectUtils.getUserProjectPermission(MEMBER, PROJECT));
			assertEquals(3, SecurityEngineUtils.getUserEnginePermission(MEMBER, "auditengine"));
		}
	}

	@Test
	void groupManagerAuditSeesCommittedManagerState() throws Exception {
		AdminSecurityGroupUtils groups = AdminSecurityGroupUtils.getInstance(actor);
		groups.addGroup(actor, "auditteam", "CUSTOM", "Audit team");
		try (var utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
				JsonObject event = AUDIT_JSON.toJsonTree(call.getArgument(0)).getAsJsonObject();
				boolean added = event.get("eventType").getAsString().equals("GROUP_MANAGER_ADD");
				assertEquals(added, SecurityGroupManagerUtils.managerExists("auditteam", MEMBER, "NATIVE"));
				assertEquals(MEMBER, event.get("subjectUserId").getAsString());
				assertEquals("auditteam", event.get("targetId").getAsString());
				return null;
			});
			SecurityGroupManagerUtils.addGroupManager(groups, actor, "auditteam", MEMBER, "NATIVE");
			SecurityGroupManagerUtils.removeGroupManager(groups, "auditteam", MEMBER, "NATIVE");
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(2));
		}
	}
}
