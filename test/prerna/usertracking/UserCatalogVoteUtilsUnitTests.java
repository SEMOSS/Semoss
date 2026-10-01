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
package prerna.usertracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class UserCatalogVoteUtilsUnitTests {

	@Test
	void votesInsertUpdateAndDeleteAcrossCredentials() throws Exception {
		try (var db = new JdbcTestDatabase(); var votes = mockStatic(UserCatalogVoteUtils.class, CALLS_REAL_METHODS)) {
			db.execute(
					"CREATE TABLE USER_CATALOG_VOTES (USERID VARCHAR, TYPE VARCHAR, ENGINEID VARCHAR, VOTE INT, LAST_MODIFIED TIMESTAMP)");
			var creds = List.of(Pair.with("u", "NATIVE"));
			votes.when(() -> UserCatalogVoteUtils.getVote(creds, "engine")).thenReturn(Map.of());
			UserCatalogVoteUtils.vote(creds, "engine", 1);
			assertEquals(1, db.value("SELECT VOTE FROM USER_CATALOG_VOTES"));
			votes.when(() -> UserCatalogVoteUtils.getVote(creds, "engine")).thenReturn(Map.of(creds.get(0), 1));
			UserCatalogVoteUtils.vote(creds, "engine", -1);
			assertEquals(-1, db.value("SELECT VOTE FROM USER_CATALOG_VOTES"));
			UserCatalogVoteUtils.delete(creds, "engine");
			assertEquals(0, db.count("USER_CATALOG_VOTES"));
		}
	}

	@Test
	void batchFailureRollsBackEarlierVotesAndKeepsErrorMessage() throws Exception {
		try (var db = new JdbcTestDatabase(); var votes = mockStatic(UserCatalogVoteUtils.class, CALLS_REAL_METHODS)) {
			db.execute(
					"CREATE TABLE USER_CATALOG_VOTES (USERID VARCHAR CHECK (USERID <> 'reject'), TYPE VARCHAR, ENGINEID VARCHAR, VOTE INT, LAST_MODIFIED TIMESTAMP)");
			var creds = List.of(Pair.with("ok", "NATIVE"), Pair.with("reject", "NATIVE"));
			votes.when(() -> UserCatalogVoteUtils.getVote(creds, "engine")).thenReturn(Map.of());
			var error = assertThrows(IllegalArgumentException.class,
					() -> UserCatalogVoteUtils.vote(creds, "engine", 1));
			assertEquals("An error occurred while saving the user's vote. See logs for details.", error.getMessage());
			assertEquals(0, db.count("USER_CATALOG_VOTES"));
			verify(db.connection).rollback();
		}
	}
}
