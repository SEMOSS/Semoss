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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftMessageDisplay;
import prerna.sablecc2.om.execptions.SemossPixelException;

/** An owner-selected message attached to a work action. */
public final class BrainExplicitSource {
	private BrainExplicitSource() {
	}

	public static Map<String, Object> save(User user, Map<String, Object> args, String origin, boolean step,
			String emailId, String chatId, String messageId) {
		BrainMailHeaderSource headers = BrainMailHeaderSource.current();
		BrainMessageSource messages = BrainMessageSource.current();
		return save(user, args, origin, step, emailId, chatId, messageId, headers, messages);
	}

	static Map<String, Object> save(User user, Map<String, Object> args, String origin, boolean step,
			String emailId, String chatId, String messageId, BrainMailHeaderSource headers,
			BrainMessageSource messages) {
		emailId = clean(emailId);
		chatId = clean(chatId);
		messageId = clean(messageId);
		if (emailId == null && chatId == null && messageId == null) {
			return step ? WorkTaskUtils.addStep(user, args, origin) : WorkTaskUtils.saveWork(user, args, origin);
		}
		if ((emailId != null && (chatId != null || messageId != null))
				|| (emailId == null && (chatId == null || messageId == null))) {
			throw new IllegalArgumentException("Pass either emailId, or both chatId and messageId.");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String source = emailId == null ? "teams" : "email";
		String graphId = emailId == null ? messageId : emailId;
		if (graphId.startsWith("email:")) {
			String key = graphId.substring("email:".length());
			graphId = CollaborationDbUtils.queryOne("SELECT GRAPH_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND MESSAGE_KEY = ?", rs -> rs.getString(1), ownerId, ownerType, key);
			if (graphId == null) {
				throw new IllegalArgumentException("The email reference is not available in Brain.");
			}
		}
		Map<String, Object> header;
		Map<String, Object> body;
		Map<String, Object> me;
		List<String> aliases;
		try {
			header = "email".equals(source) ? headers.message(user, graphId)
					: headers.chatMessage(user, chatId, graphId);
			if (header == null || !graphId.equals(header.get("id"))) {
				throw new IllegalArgumentException("The selected message is not available.");
			}
			checkHeader(ownerId, ownerType, source, header);
			body = messages.fetch(user, source, chatId, graphId);
			if (body == null) {
				throw new IllegalArgumentException("The selected message is no longer available.");
			}
			checkKeyword(ownerId, ownerType, header, body);
			if ("email".equals(source)) {
				BrainMailFolders.Tree tree = BrainMailFolders.current(user, headers, ownerId, ownerType);
				tree = BrainMailFolders.covering(user, headers, ownerId, ownerType, tree, List.of(header));
				if (BrainMailFolders.keep(tree, List.of(header)).isEmpty()) {
					throw new IllegalArgumentException("The selected email is in a folder Brain does not ingest.");
				}
			}
			me = headers.me(user);
			aliases = headers.aliases(user);
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Could not check the selected message: " + e.getMessage(), e);
		}
		String myAddress = BrainRulesGate.norm(BrainMailImport.first(me, "mail", "userPrincipalName"));
		if (myAddress == null) {
			throw new IllegalStateException("The signed-in mailbox has no address.");
		}
		Map<String, Object> selected = new LinkedHashMap<>(header);
		selected.put(BrainMailImport.SOURCE_KEY, source);
		String key = CollaborationDbUtils.deterministicId(ownerId, ownerType, source,
				BrainMailImport.messageId(selected));
		AtomicReference<Map<String, Object>> result = new AtomicReference<>();
		synchronized (CollaborationDbUtils.ownerLock(BrainSync.INGEST_LOCK, ownerId, ownerType)) {
			if (!BrainSync.canIngest(ownerId, ownerType)) {
				throw new IllegalStateException("Brain import must be set up and idle before adding a message.");
			}
			CollaborationDbUtils.batch(conn -> {
				checkHeader(ownerId, ownerType, source, selected);
				checkKeyword(ownerId, ownerType, selected, body);
				String decision = CollaborationDbUtils.queryOne("SELECT DECISION FROM BRAIN_MESSAGE WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND MESSAGE_KEY = ?", rs -> rs.getString(1), ownerId, ownerType, key);
				if (BrainRulesGate.NEVER.equals(decision)) {
					throw blocked();
				}
				BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType, false);
				run.ensureSelf(myAddress, (String) me.get("displayName"),
						(String) me.get("userPrincipalName"), aliases);
				run.importOne(selected);
				String[] row = CollaborationDbUtils.queryOne("SELECT THREAD_ID, SENDER_PERSON_ID, DECISION "
						+ "FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?",
						rs -> new String[] { rs.getString(1), rs.getString(2), rs.getString(3) }, ownerId, ownerType,
						key);
				if (row == null || row[0] == null || BrainRulesGate.NEVER.equals(row[2])) {
					throw new IllegalArgumentException("Brain could not ingest the selected message.");
				}
				includeTopicRules(user, ownerId, ownerType, row[0], row[1], source);
				run.refreshThreads();
				Map<String, Object> workArgs = new LinkedHashMap<>(args);
				if (step) {
					String existing = clean(CollaborationDbUtils.asString(workArgs.get("threadId")));
					if (existing != null && !existing.equals(row[0])) {
						throw new IllegalArgumentException("The step's thread does not match the selected message.");
					}
					workArgs.put("threadId", row[0]);
				} else {
					List<String> threads = new ArrayList<>();
					if (workArgs.get("threadIds") instanceof List<?> values) {
						for (Object value : values) {
							threads.add(String.valueOf(value));
						}
					}
					if (!threads.contains(row[0])) {
						threads.add(row[0]);
					}
					workArgs.put("threadIds", threads);
				}
				result.set(step ? WorkTaskUtils.addStep(user, workArgs, origin)
						: WorkTaskUtils.saveWork(user, workArgs, origin));
			});
		}
		return result.get();
	}

	private static void checkHeader(String ownerId, String ownerType, String source, Map<String, Object> header) {
		String from = BrainMailImport.address(header.get("from"));
		if (from == null) {
			throw new IllegalArgumentException("Cannot check the selected message: its sender is unavailable.");
		}
		String folder = CollaborationDbUtils.asString(header.get("parentFolderId"));
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		if (BrainRulesGate.neverRule(rules, from, BrainRulesGate.findPerson(ownerId, ownerType, from), folder) != null) {
			throw blocked();
		}
		if ("email".equals(source) && folder == null
				&& rules.stream().anyMatch(rule -> BrainRuleUtils.NEVER_FOLDER.equals(rule.kind()))) {
			throw new IllegalArgumentException("Cannot check the selected email's folder against never-ingest rules.");
		}
	}

	private static void checkKeyword(String ownerId, String ownerType, Map<String, Object> header,
			Map<String, Object> body) {
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		if (!(body.get("body") instanceof Map<?, ?>)
				&& rules.stream().anyMatch(rule -> BrainRuleUtils.NEVER_KEYWORD.equals(rule.kind()))) {
			throw new IllegalArgumentException("Cannot check the selected message's body against never-ingest rules.");
		}
		if (BrainRulesGate.keywordRule(rules,
				CollaborationDbUtils.asString(header.get("subject")), MicrosoftMessageDisplay.text(body)) != null) {
			throw blocked();
		}
	}

	private static void includeTopicRules(User user, String ownerId, String ownerType, String threadId,
			String personId, String source) {
		if (personId == null) {
			throw new IllegalArgumentException("The selected message's sender could not be identified.");
		}
		List<String> topics = CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> rs.getString(1), ownerId, ownerType, threadId);
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		if (rules.stream().anyMatch(rule -> BrainRuleUtils.EXCLUDE_CHANNEL.equals(rule.kind())
				&& source.equals(rule.channel()) && personId.equals(rule.personId()))) {
			throw new IllegalArgumentException("The selected sender is excluded on this channel.");
		}
		String[] participant = CollaborationDbUtils.queryOne("SELECT INCLUDED, EXCLUDED_BY FROM BRAIN_THREAD_PARTICIPANT "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
				rs -> new String[] { String.valueOf(CollaborationDbUtils.getBoolean(rs, "INCLUDED")), rs.getString(2) },
				ownerId, ownerType, threadId, personId);
		if (participant != null && "false".equals(participant[0]) && !BrainThreadUtils.RULE.equals(participant[1])) {
			throw new IllegalArgumentException("The selected sender was manually excluded from this thread.");
		}
		if (!BrainRuleUtils.activeTopicRulesForPerson(ownerId, ownerType, threadId, personId).isEmpty()) {
			BrainThreadUtils.setThreadParticipant(user, threadId, personId, true);
		}
		Boolean included = CollaborationDbUtils.queryOne("SELECT INCLUDED FROM BRAIN_THREAD_PARTICIPANT "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
				rs -> CollaborationDbUtils.getBoolean(rs, "INCLUDED"), ownerId, ownerType, threadId, personId);
		if (Boolean.FALSE.equals(included) || BrainRulesGate.exclusionRule(rules, topics, source, personId,
				BrainRuleUtils.threadRuleOverrides(ownerId, ownerType, threadId, personId)) != null) {
			throw new IllegalArgumentException("The selected message is excluded from this thread.");
		}
	}

	private static SemossPixelException blocked() {
		String message = "The selected message is blocked by a global never-ingest rule.";
		return new SemossPixelException(message, Map.of("code", "NEVER_INGEST", "message", message));
	}

	private static String clean(String value) {
		return value == null || value.isBlank() ? null : value.trim();
	}
}
