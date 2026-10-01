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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.JdbcTestDatabase;

class SecurityAdminExternalConnectorsUtilsUnitTests {

	@Test
	void adminGateValidationAndNormalizedInsertUseExistingContract() throws Exception {
		try (var db = new JdbcTestDatabase();
				var admins = mockStatic(SecurityAdminExternalConnectorsUtils.class, CALLS_REAL_METHODS);
				var wrappers = mockStatic(WrapperManager.class)) {
			User user = mock(User.class);
			admins.when(() -> SecurityAdminExternalConnectorsUtils.userIsAdmin(user)).thenReturn(false);
			assertNull(SecurityAdminExternalConnectorsUtils.getInstance(user));
			admins.when(() -> SecurityAdminExternalConnectorsUtils.userIsAdmin(user)).thenReturn(true);
			var admin = SecurityAdminExternalConnectorsUtils.getInstance(user);
			assertThrows(IllegalArgumentException.class,
					() -> admin.insertSalesforceConnection(" ", "secret", "alias"));
			verify(db.engine, never()).getConnection();
			var manager = mock(WrapperManager.class);
			var rows = mock(IRawSelectWrapper.class);
			wrappers.when(WrapperManager::getInstance).thenReturn(manager);
			when(manager.getRawWrapper(eq(db.engine), any(SelectQueryStruct.class))).thenReturn(rows);
			db.execute(
					"CREATE TABLE SALESFORCE_CONNECTIONS (ID VARCHAR, ALIAS VARCHAR, CLIENTID VARCHAR, CLIENTSECRET VARCHAR)");
			assertNotNull(admin.insertSalesforceConnection(" client ", " secret ", " alias "));
			assertEquals("client", db.value("SELECT CLIENTID FROM SALESFORCE_CONNECTIONS"));
			assertEquals("secret", db.value("SELECT CLIENTSECRET FROM SALESFORCE_CONNECTIONS"));
			db.execute(
					"CREATE TABLE SERVICENOW_CONNECTIONS (ID VARCHAR, INSTANCEURL VARCHAR, ALIAS VARCHAR, CLIENTID VARCHAR, CLIENTSECRET CLOB, USERPROFILEURL VARCHAR)",
					"CREATE TABLE JIRA_CONNECTIONS (ID VARCHAR, ALIAS VARCHAR, CLIENTID VARCHAR, CLIENTSECRET CLOB, SCOPE VARCHAR, USERPROFILEURL VARCHAR)");
			assertNotNull(admin.insertServiceNowConnection(" https://instance.test ", " alias ", " client ", " secret ",
					" https://profile.test "));
			assertNotNull(
					admin.insertJiraConnection(" alias ", " client ", " secret ", " scope ", " https://profile.test "));
			assertEquals(1, db.count("SERVICENOW_CONNECTIONS"));
			assertEquals(1, db.count("JIRA_CONNECTIONS"));
			db.execute("DROP TABLE SALESFORCE_CONNECTIONS");
			db.manual();
			assertThrows(RuntimeException.class, () -> admin.insertSalesforceConnection("client", "secret", "alias"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}
}
