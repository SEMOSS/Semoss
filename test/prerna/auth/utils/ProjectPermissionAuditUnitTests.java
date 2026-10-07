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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.JdbcTestDatabase;
import prerna.util.Utility;

class ProjectPermissionAuditUnitTests {

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void bulkUpdateAuditsOnlyAfterSuccessfulCommit(boolean succeeds) throws Exception {
		User actor = mock(User.class);
		List<Map<String, String>> requests = List.of(
				Map.of("userid", "member", "type", "NATIVE", "permission", "EDIT"));
		try (var db = new JdbcTestDatabase(); var users = mockStatic(User.class);
				var projects = mockStatic(SecurityProjectUtils.class, CALLS_REAL_METHODS);
				var utility = mockStatic(Utility.class); var audit = mockStatic(UserAuditTrailUtils.class)) {
			db.execute("CREATE TABLE PROJECTPERMISSION (USERID VARCHAR, PROJECTID VARCHAR, PERMISSION INT, "
					+ "PERMISSIONGRANTEDBY VARCHAR, PERMISSIONGRANTEDBYTYPE VARCHAR, DATEADDED TIMESTAMP, ENDDATE TIMESTAMP)",
					"INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION) VALUES ('member', 'project', "
							+ AccessPermissionEnum.READ_ONLY.getId() + ")");
			db.manual();
			users.when(() -> User.getPrimaryUserIdAndTypePair(actor)).thenReturn(Pair.with("admin", "NATIVE"));
			projects.when(() -> SecurityProjectUtils.getMaxUserProjectPermission(actor, "project"))
					.thenReturn(AccessPermissionEnum.OWNER.getId());
			projects.when(() -> SecurityProjectUtils.getUserProjectPermissions(List.of("member"), "project"))
					.thenReturn(Map.of("member", AccessPermissionEnum.READ_ONLY.getId()));
			projects.when(() -> SecurityProjectUtils.getUserProjectPermission("member", "project"))
					.thenReturn(AccessPermissionEnum.READ_ONLY.getId());
			if (succeeds) {
				when(db.engine.getPreparedStatement(anyString())).thenAnswer(invocation ->
						db.connection.prepareStatement(invocation.getArgument(0, String.class)));
			} else {
				when(db.engine.getPreparedStatement(anyString())).thenThrow(new SQLException("Write failed"));
			}
			audit.when(() -> UserAuditTrailUtils.recordPermissionUpdate(actor, "PROJECT", "project", null,
					"project", null, null, "member", "NATIVE", "READ_ONLY", "EDIT", null)).thenAnswer(invocation -> {
				// Rolling back here proves the permission was committed before auditing.
				db.connection.rollback();
				assertEquals(AccessPermissionEnum.EDIT.getId(), db.value("SELECT PERMISSION FROM PROJECTPERMISSION"));
				return null;
			});
			assertDoesNotThrow(() -> SecurityProjectUtils.editProjectUserPermissions(actor, "project", requests, null));
			if (succeeds) {
				audit.verify(() -> UserAuditTrailUtils.recordPermissionUpdate(actor, "PROJECT", "project", null,
						"project", null, null, "member", "NATIVE", "READ_ONLY", "EDIT", null));
			} else {
				audit.verifyNoInteractions();
			}
		}
	}
}
