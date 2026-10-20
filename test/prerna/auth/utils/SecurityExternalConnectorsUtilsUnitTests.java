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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.JdbcTestDatabase;
import prerna.util.QueryExecutionUtility;

class SecurityExternalConnectorsUtilsUnitTests {

	@Test
	void appInsertPreservesPemWhitespaceNullAndEmptySecrets() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of());
			db.execute(
					"CREATE TABLE GITHUB_APP (APP_ID BIGINT, SLUG VARCHAR, APP_NAME VARCHAR, OWNER_LOGIN VARCHAR, HTML_URL VARCHAR, WEBHOOK_URL VARCHAR, CLIENT_ID VARCHAR, CLIENT_SECRET CLOB, WEBHOOK_SECRET CLOB, PRIVATE_KEY CLOB, CREATED_ON TIMESTAMP, UPDATED_ON TIMESTAMP)");
			db.execute("CREATE TABLE GITHUB_PROJECT_LINK (APP_ID BIGINT)");
			String pem = "  test-only key payload\n";
			SecurityExternalConnectorsUtils.upsertGitHubApp(1, " slug ", "name", null, null, null, null, null, "", pem);
			assertEquals("slug", db.value("SELECT SLUG FROM GITHUB_APP"));
			assertNull(db.value("SELECT CLIENT_SECRET FROM GITHUB_APP"));
			assertEquals("", db.value("SELECT WEBHOOK_SECRET FROM GITHUB_APP"));
			assertEquals(pem, db.value("SELECT PRIVATE_KEY FROM GITHUB_APP"));
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of(java.util.Map.of("appId", 1)));
			SecurityExternalConnectorsUtils.upsertGitHubApp(1, "changed", "new", null, null, null, null, "secret", null,
					null);
			assertEquals("changed", db.value("SELECT SLUG FROM GITHUB_APP"));
			assertNull(db.value("SELECT PRIVATE_KEY FROM GITHUB_APP"));
			SecurityExternalConnectorsUtils.deleteGitHubApp(1);
			assertEquals(0, db.count("GITHUB_APP"));
		}
	}

	@Test
	void validationAndSqlFailureKeepTheirContracts() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			assertThrows(IllegalArgumentException.class, () -> SecurityExternalConnectorsUtils.upsertGitHubApp(1, " ",
					null, null, null, null, null, null, null, null));
			verify(db.engine, never()).getConnection();
			db.manual();
			assertThrows(java.sql.SQLException.class, () -> SecurityExternalConnectorsUtils.deleteGitHubApp(1));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void projectLinkInsertUpdateAndBranchChangesPreserveNormalization() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			db.execute(
					"CREATE TABLE GITHUB_PROJECT_LINK (PROJECT_ID VARCHAR, APP_ID BIGINT, INSTALLATION_ID BIGINT, REPO_ID BIGINT, REPO_FULL_NAME VARCHAR, BRANCH VARCHAR, SUBDIR VARCHAR, CREATED_ON TIMESTAMP, UPDATED_ON TIMESTAMP)");
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of());
			SecurityExternalConnectorsUtils.upsertGitHubProjectLink(" p ", 1, 2, 3, "owner/repo", " main ", " ");
			assertNull(db.value("SELECT SUBDIR FROM GITHUB_PROJECT_LINK"));
			assertEquals("main", db.value("SELECT BRANCH FROM GITHUB_PROJECT_LINK"));
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of(java.util.Map.of("projectId", "p")));
			SecurityExternalConnectorsUtils.upsertGitHubProjectLink("p", 1, 2, 3, "owner/repo", "dev", " src ");
			assertEquals("src", db.value("SELECT SUBDIR FROM GITHUB_PROJECT_LINK"));
			SecurityExternalConnectorsUtils.updateGitHubProjectLinkBranch(" p ", " branch ");
			assertEquals("branch", db.value("SELECT BRANCH FROM GITHUB_PROJECT_LINK"));
			SecurityExternalConnectorsUtils.deleteGitHubProjectLink(" p ");
			assertEquals(0, db.count("GITHUB_PROJECT_LINK"));
		}
	}

	@Test
	void graphReplacementFailureRestoresTokensAndRefreshKeepsExactText() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE MS_GRAPH_SUBSCRIPTION (SUBSCRIPTION_ID VARCHAR, USER_ID VARCHAR, USER_PROVIDER VARCHAR, USER_EMAIL VARCHAR, CLIENT_STATE VARCHAR CHECK (CLIENT_STATE <> 'reject'), RESOURCE VARCHAR, CHANGE_TYPE VARCHAR, NOTIFICATION_URL VARCHAR, EXPIRATION TIMESTAMP, ACCESS_TOKEN CLOB, REFRESH_TOKEN CLOB, TOKEN_EXPIRATION TIMESTAMP, CREATED_ON TIMESTAMP, UPDATED_ON TIMESTAMP)");
			SecurityExternalConnectorsUtils.upsertMicrosoftGraphSubscription(" sub ", "u", "MS", null, "ok", "resource",
					"updated", "https://test.invalid", null, " old ", null, null);
			assertThrows(RuntimeException.class,
					() -> SecurityExternalConnectorsUtils.upsertMicrosoftGraphSubscription("sub", "u", "MS", null,
							"reject", "resource", "updated", null, null, "new", "new", null));
			assertEquals(" old ", db.value("SELECT ACCESS_TOKEN FROM MS_GRAPH_SUBSCRIPTION"));
			SecurityExternalConnectorsUtils.updateMicrosoftGraphSubscriptionToken("sub", "", "  refreshed  ", null);
			assertEquals("", db.value("SELECT ACCESS_TOKEN FROM MS_GRAPH_SUBSCRIPTION"));
			assertEquals("  refreshed  ", db.value("SELECT REFRESH_TOKEN FROM MS_GRAPH_SUBSCRIPTION"));
			var expiry = java.sql.Timestamp.valueOf("2026-01-02 03:04:05");
			SecurityExternalConnectorsUtils.updateMicrosoftGraphSubscriptionExpiration("sub", expiry);
			assertEquals(expiry, db.value("SELECT EXPIRATION FROM MS_GRAPH_SUBSCRIPTION"));
			SecurityExternalConnectorsUtils.deleteMicrosoftGraphSubscription("sub");
			assertEquals(0, db.count("MS_GRAPH_SUBSCRIPTION"));
		}
	}
}
