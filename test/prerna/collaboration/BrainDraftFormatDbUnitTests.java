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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Learn my style against an in-memory H2 Collaboration schema: the signature and font it copies from sent mail, and
 * the ones it leaves alone.
 */
class BrainDraftFormatDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<BrainThreadClassifier> classifier;
	private MockedStatic<BrainMessageSource> source;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_draft_format_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		AccessToken token = new AccessToken();
		token.setId("owner-" + UUID.randomUUID());
		token.setName("Rob Weiler");
		token.setEmail("rob@test.com");
		token.setProvider(AuthProvider.MICROSOFT);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		// the settings read names the platform classifier, which comes from RDF_Map
		classifier = mockStatic(BrainThreadClassifier.class);
		// one email the owner sent, signed and written in Georgia
		sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, GRAPH_ID, FOLDER, RECEIVED_AT) "
				+ "VALUES (?, ?, 'k-1', 'g-1', 'sentitems', ?)", ownerId, ownerType, Timestamp.from(Instant.now()));
		Map<String, Object> sent = Map.of("subject", "Budget", "uniqueBody", Map.of("contentType", "html", "content",
				"<div style=\"font-family: Georgia; font-size: 11pt\">See the attached budget.</div>"
						+ "<div id=\"Signature\"><p>Rob Weiler</p><p>Acme</p></div>"));
		BrainMessageSource mail = (u, channel, conversationId, graphId) -> sent;
		source = mockStatic(BrainMessageSource.class);
		source.when(BrainMessageSource::current).thenReturn(mail);
	}

	@AfterEach
	void tearDown() throws Exception {
		source.close();
		classifier.close();
		registry.close();
		connection.close();
	}

	@Test
	void learnMyStyleFillsOnlyWhatWasNeverSetAndLeavesWhatTheOwnerCleared() {
		BrainDraftFormat.learnInto(user, true);
		Map<String, Object> learned = BrainProfileUtils.getSettings(ownerId, ownerType);
		assertTrue(((String) learned.get("draftSignatureHtml")).contains("Rob Weiler"));
		assertEquals("Georgia", learned.get("draftFont"));

		// Remove signature, and a font reset to the default
		BrainProfileUtils.saveSettings(user, Map.of("draftSignatureHtml", "", "draftFont", ""));
		BrainDraftFormat.learnInto(user, true);

		Map<String, Object> cleared = BrainProfileUtils.getSettings(ownerId, ownerType);
		assertEquals("", cleared.get("draftSignatureHtml"));
		assertEquals(BrainDraftFormat.DEFAULT_FONT, cleared.get("draftFont"));
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}
}
