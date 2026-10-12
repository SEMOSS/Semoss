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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;

class InboxBriefFilterUnitTests {

	private static final Pair<String, String> OWNER = Pair.with("owner", "NATIVE");
	private static final Timestamp NOW = Timestamp.valueOf("2026-10-12 12:00:00");
	private JdbcTestDatabase db;
	private final List<String> queried = new ArrayList<>();

	@BeforeEach
	void setup() throws Exception {
		db = new JdbcTestDatabase();
		db.registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(db.engine);
		when(db.engine.getPreparedStatement(anyString())).thenAnswer(call -> {
			String sql = call.getArgument(0);
			queried.add(sql);
			return db.connection.prepareStatement(sql);
		});
		Set<String> tables = Set.of("BRAIN_RULE", "BRAIN_PERSON", "BRAIN_PERSON_ADDRESS", "BRAIN_THREAD",
				"BRAIN_THREAD_ALIAS", "BRAIN_THREAD_TOPIC", "BRAIN_THREAD_PARTICIPANT");
		for (var table : new CollaborationOwlCreator(db.engine.getQueryUtil()).getDBSchema()) {
			if (tables.contains(table.getValue0())) {
				db.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", table.getValue1().stream()
						.map(column -> column.getValue0() + " " + column.getValue1()).toList()) + ")");
			}
		}
	}

	@AfterEach
	void cleanup() throws Exception {
		db.close();
	}

	@Test
	void directSenderDomainAndOpaqueFolderRulesApplyToUnimportedMail() throws Exception {
		rule("sender", "never_sender", "blocked@example.test", null, null, null);
		rule("domain", "never_domain", "private.test", null, null, null);
		rule("folder", "never_folder", "FolderCase", null, null, null);
		Map<String, Object> blockedFolder = message("folder", "someone@other.test", "unknown");
		blockedFolder.put("parentFolderId", "FolderCase");
		Map<String, Object> allowedFolder = message("allowed", "someone@other.test", "unknown");
		allowedFolder.put("parentFolderId", "foldercase");
		assertEquals(List.of("allowed"), ids(InboxBriefFilter.filter(OWNER, List.of(
				message("sender", "  BLOCKED@EXAMPLE.TEST  ", "unknown"),
				message("domain", "person@sub.private.test", "unknown"), blockedFolder, allowedFolder))));
	}

	@Test
	void personExclusionsResolveAliasesPrimaryAddressesAndLegacyNeverIngest() throws Exception {
		person("global", "primary@example.test", false);
		alias("global", "alias@example.test");
		person("muted", "muted@example.test", false);
		alias("muted", "muted-alias@example.test");
		person("legacy", "legacy@example.test", true);
		rule("global", "exclude_everywhere", null, null, "global", null);
		rule("muted", "mute_sender", null, null, "muted", null);
		rule("address", "mute_sender", "quiet@example.test", null, null, null);
		assertEquals(List.of("allowed"), ids(InboxBriefFilter.filter(OWNER, List.of(
				message("alias", " ALIAS@EXAMPLE.TEST ", "unknown"),
				message("primary", "primary@example.test", "unknown"),
				message("muted", "muted-alias@example.test", "unknown"),
				message("address", "quiet@example.test", "unknown"),
				message("legacy", "legacy@example.test", "unknown"),
				message("allowed", "other@example.test", "unknown")))));
	}

	@Test
	void topicExclusionsUseConversationAliasesWhileNewTopicsRemainEligible() throws Exception {
		person("person", "sender@example.test", false);
		thread("known", "email:known-conversation", false, false);
		execute("INSERT INTO BRAIN_THREAD_ALIAS (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY) VALUES (?, ?, ?, ?)",
				"owner", "NATIVE", "known", "email:joined-conversation");
		execute("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID) VALUES (?, ?, ?, ?)",
				"owner", "NATIVE", "known", "excluded-topic");
		rule("topic", "exclude_topic", null, "excluded-topic", "person", null);
		assertEquals(List.of("new", "different-person"), ids(InboxBriefFilter.filter(OWNER, List.of(
				message("joined", "sender@example.test", "joined-conversation"),
				message("known", "sender@example.test", "known-conversation"),
				message("new", "sender@example.test", "new-topic-conversation"),
				message("different-person", "other@example.test", "known-conversation")))));
	}

	@ParameterizedTest
	@ValueSource(strings = { "email", "outlook" })
	void broadAndPersonChannelRulesExcludeOnlyEmail(String source) throws Exception {
		person("person", "sender@example.test", false);
		rule("teams", "exclude_channel", null, null, null, "teams");
		rule("person", "exclude_channel", null, null, "person", source);
		List<Map<String, Object>> mail = List.of(message("known", "sender@example.test", "new"),
				message("other", "other@example.test", "new"));
		assertEquals(List.of("other"), ids(InboxBriefFilter.filter(OWNER, mail)));
		// Legacy broad channel rules can store the channel in VALUE.
		rule("broad", "exclude_channel", source, null, null, null);
		assertTrue(InboxBriefFilter.filter(OWNER, mail).isEmpty());
	}

	@Test
	void broadTopicsParticipantFlagsAndExplicitMutesApplyWithoutClassifierWhitelist() throws Exception {
		person("person", "sender@example.test", false);
		thread("topic", "email:topic", false, false);
		thread("participant", "email:participant", false, false);
		thread("muted", "outlook:muted", true, false);
		thread("automated", "email:automated", false, true);
		execute("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID) VALUES (?, ?, ?, ?)",
				"owner", "NATIVE", "topic", "private-topic");
		rule("broad-topic", "exclude_topic", "private-topic", null, null, null);
		execute("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, INCLUDED) VALUES (?, ?, ?, ?, ?)",
				"owner", "NATIVE", "participant", "person", false);
		execute("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, INCLUDED) VALUES (?, ?, ?, ?, ?)",
				"owner", "NATIVE", "automated", "person", null);
		assertEquals(List.of("automated", "new"), ids(InboxBriefFilter.filter(OWNER, List.of(
				message("topic", "unknown@example.test", "topic"),
				message("participant", "sender@example.test", "participant"),
				message("muted", "sender@example.test", "muted"),
				message("automated", "sender@example.test", "automated"),
				message("new", "sender@example.test", "new")))));
	}

	@Test
	void absentConversationUsesTheImportsMessageIdFallback() throws Exception {
		thread("single", "email:<single@example.test>", true, false);
		Map<String, Object> single = message("graph-id", "sender@example.test", null);
		single.put("internetMessageId", "<single@example.test>");
		assertTrue(InboxBriefFilter.filter(OWNER, List.of(single)).isEmpty());
	}

	@Test
	void keywordsMatchCleanHtmlBeforeExcerptTruncationAndKeepWordBoundaries() throws Exception {
		rule("keyword", "never_keyword", "private project", null, null, null);
		Map<String, Object> longBody = message("long", "sender@example.test", "new");
		longBody.put("body", Map.of("contentType", "html", "content",
				"<p>" + "a ".repeat(800) + "PRIVATE <b>PROJECT</b></p>"));
		Map<String, Object> subject = message("subject", "sender@example.test", "new");
		subject.put("subject", "RE: Private Project update");
		Map<String, Object> allowed = message("allowed", "sender@example.test", "new");
		allowed.put("body", Map.of("contentType", "text", "content", "Private projects are reviewed."));
		assertEquals(List.of("allowed"), ids(InboxBriefFilter.filter(OWNER, List.of(longBody, subject, allowed))));
	}

	@Test
	void cleanCopiesCannotRestoreQuotedHistoryThroughPreviewFallback() throws Exception {
		rule("keyword", "never_keyword", "private project", null, null, null);
		Map<String, Object> source = message("clean", "sender@example.test", "new");
		source.put("subject", "RE: Update");
		source.put("body", Map.of("contentType", "html", "content",
				"<div id='Signature'>Private Project</div><blockquote>Private Project</blockquote>"));
		source.put("bodyPreview", "Private Project");
		Map<String, Object> filtered = InboxBriefFilter.filter(OWNER, List.of(source)).get(0);
		assertNotSame(source, filtered);
		assertEquals("Update", filtered.get("subject"));
		assertEquals("", ((Map<?, ?>) filtered.get("body")).get("content"));
		assertEquals("", filtered.get("bodyPreview"));
		assertEquals("Private Project", source.get("bodyPreview"));
	}

	@Test
	void ownerAndProviderBoundariesDisabledRulesAndReadOnlyContractHold() throws Exception {
		person("person", "sender@example.test", false);
		thread("thread", "email:conversation", false, false);
		rule("disabled", "never_sender", "sender@example.test", null, null, null);
		execute("UPDATE BRAIN_RULE SET DISABLED_AT = ? WHERE RULE_ID = ?", NOW, "disabled");
		for (Pair<String, String> other : List.of(Pair.with("other-owner", "NATIVE"), Pair.with("owner", "OTHER"))) {
			execute("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, VALUE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
					other.getValue0(), other.getValue1(), "other-rule", "never_sender", "sender@example.test", NOW);
			execute("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, EMAIL_NORM, NEVER_INGEST) VALUES (?, ?, ?, ?, ?)",
					other.getValue0(), other.getValue1(), "person", "sender@example.test", true);
			execute("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, MUTED) VALUES (?, ?, ?, ?, ?)",
					other.getValue0(), other.getValue1(), "thread", "email:conversation", true);
			execute("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, INCLUDED) VALUES (?, ?, ?, ?, ?)",
					other.getValue0(), other.getValue1(), "thread", "person", false);
		}
		assertEquals(List.of("allowed"), ids(InboxBriefFilter.filter(OWNER,
				List.of(message("allowed", "sender@example.test", "conversation")))));
		assertFalse(queried.isEmpty());
		assertTrue(queried.stream().allMatch(sql -> sql.startsWith("SELECT ")));
		assertTrue(queried.stream().noneMatch(sql -> sql.contains("SOURCE_CONNECTION") || sql.contains("BRAIN_MESSAGE")));
	}

	private void person(String id, String address, boolean neverIngest) throws Exception {
		execute("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, EMAIL_NORM, NEVER_INGEST) VALUES (?, ?, ?, ?, ?)",
				"owner", "NATIVE", id, address, neverIngest);
	}

	private void alias(String person, String address) throws Exception {
		execute("INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, VALUE_NORM) VALUES (?, ?, ?, ?)",
				"owner", "NATIVE", person, address);
	}

	private void thread(String id, String key, boolean muted, boolean automated) throws Exception {
		execute("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, MUTED, AUTOMATED) VALUES (?, ?, ?, ?, ?, ?)",
				"owner", "NATIVE", id, key, muted, automated);
	}

	private void rule(String id, String kind, String value, String topic, String person, String channel) throws Exception {
		execute("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, VALUE, TOPIC_ID, PERSON_ID, CHANNEL, CREATED_AT) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", "owner", "NATIVE", id, kind, value, topic, person, channel, NOW);
	}

	private void execute(String sql, Object... values) throws Exception {
		try (PreparedStatement statement = db.connection.prepareStatement(sql)) {
			for (int i = 0; i < values.length; i++) {
				statement.setObject(i + 1, values[i]);
			}
			statement.executeUpdate();
		}
	}

	private static Map<String, Object> message(String id, String from, String conversation) {
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("id", id);
		message.put("from", Map.of("emailAddress", Map.of("address", from)));
		message.put("conversationId", conversation);
		message.put("subject", "Update");
		message.put("body", Map.of("contentType", "text", "content", "Please review the update."));
		return message;
	}

	private static List<String> ids(List<Map<String, Object>> messages) {
		return messages.stream().map(message -> (String) message.get("id")).toList();
	}
}
