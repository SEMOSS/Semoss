package prerna.collaboration;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.util.Utility;

// Refreshes only previously imported, permitted headers; A.1 never substitutes aggregate recipient guesses.
final class BrainTopicOnboarding {

	static final String STRATEGY = "candidate_a1";
	private static final int MAX_PER_FOLDER = 5000;

	record Result(List<BrainTopicSuggest.Candidate> candidates, Map<String, Object> diagnostics) {
	}

	private BrainTopicOnboarding() {
	}

	static Result propose(User user, String engineId, String ownerId, String ownerType,
			Map<String, BrainTopicSuggest.Thread> eligible, String self, Map<String, String> emails,
			Set<String> vips, BrainOrgDomains.Org ownOrg, Set<String> takenNames) {
		Map<String, Object> notes = new LinkedHashMap<>();
		BrainTopicStructure.Prepared prepared = snapshot(user, ownerId, ownerType, eligible.keySet(), self, emails,
				vips, ownOrg, notes);
		IModelEngine model = Utility.getModel(engineId);
		if (model == null) {
			throw new IllegalArgumentException("The topic model could not be loaded");
		}
		BrainTopicVotes.Result votes = BrainTopicVotes.run(prepared, settings(ownerId, ownerType), (prompt, instructions, params) -> {
			Insight insight = new Insight();
			insight.setUser(user);
			return model.ask(prompt, instructions, insight, new LinkedHashMap<>(params)).getStringResponse();
		});
		Map<String, Object> diagnostics = new LinkedHashMap<>();
		diagnostics.put("strategy", STRATEGY);
		diagnostics.putAll(notes);
		diagnostics.put("status", votes.status());
		diagnostics.put("poolThreads", prepared.pool().size());
		diagnostics.put("gatedThreads", prepared.gated());
		diagnostics.put("seedTopics", prepared.seeds().size());
		diagnostics.put("droppedTopics", votes.dropped().size());
		diagnostics.put("modelCalls", votes.calls());
		diagnostics.put("failedVotes", votes.failedVotes());
		if ("vote_failed".equals(votes.status())) {
			diagnostics.put("modelError", "Topic voting did not produce three complete answers; retry later");
			return new Result(List.of(), diagnostics);
		}
		List<BrainTopicSuggest.Candidate> candidates = new ArrayList<>();
		Set<String> names = new HashSet<>(takenNames);
		Set<Integer> grouped = new HashSet<>();
		for (int i = 0; i < prepared.seeds().size(); i++) {
			if (votes.dropped().contains(i)) {
				continue;
			}
			final int topic = i;
			List<BrainTopicSuggest.Thread> members = prepared.filed().entrySet().stream()
					.filter(e -> e.getValue() == topic).map(e -> {
						grouped.add(e.getKey());
						return eligible.get(prepared.pool().get(e.getKey()).id());
					}).toList();
			String name = uniqueName(votes.names().get(i), names);
			List<String> ids = members.stream().map(BrainTopicSuggest.Thread::id).sorted().toList();
			String key = STRATEGY + ":" + CollaborationDbUtils.deterministicId(ownerId, ownerType, ids.toArray(String[]::new));
			// the card shows threads, people and your part; how it was grouped stays in diagnostics
			// no name-word keywords: a rename would leave the old words behind for the classifier
			candidates.add(new BrainTopicSuggest.Candidate(key, name, members, List.of(), null, votes.abouts().get(i)));
		}
		diagnostics.put("groupedThreads", grouped.size());
		diagnostics.put("unsortedThreads", prepared.pool().size() - grouped.size());
		return new Result(candidates, diagnostics);
	}

	// wide once a sort has run since the last import: the classifier took automated mail out of the pool
	static BrainTopicStructure.Settings settings(String ownerId, String ownerType) {
		boolean sorted = CollaborationDbUtils.exists("SELECT 1 FROM COLLAB_JOB c WHERE c.OWNER_ID = ? AND c.OWNER_TYPE = ? "
				+ "AND c.KIND = ? AND c.STATUS = ? AND c.STARTED_AT >= (SELECT MAX(i.STARTED_AT) FROM COLLAB_JOB i WHERE "
				+ "i.OWNER_ID = c.OWNER_ID AND i.OWNER_TYPE = c.OWNER_TYPE AND i.KIND = ? AND i.STATUS = ?)", ownerId,
				ownerType, BrainThreadClassifier.JOB_KIND, CollaborationJobUtils.DONE, BrainMailImport.KIND,
				CollaborationJobUtils.DONE);
		return sorted ? BrainTopicStructure.WIDE : BrainTopicStructure.A1;
	}

	private static String uniqueName(String proposed, Set<String> names) {
		String name = proposed;
		for (int suffix = 2; !names.add(name.toLowerCase(Locale.ROOT)); suffix++) {
			String[] words = proposed.split("\\s+");
			String stem = String.join(" ", java.util.Arrays.copyOf(words, Math.min(4, words.length)));
			String number = " " + suffix;
			name = stem.substring(0, Math.min(stem.length(), 60 - number.length())).trim() + number;
		}
		return name;
	}

	private static BrainTopicStructure.Prepared snapshot(User user, String ownerId, String ownerType,
			Set<String> eligible, String self, Map<String, String> emails, Set<String> vips, BrainOrgDomains.Org ownOrg,
			Map<String, Object> notes) {
		if (self == null || emails.get(self) == null) {
			throw new IllegalStateException("Import the mailbox before proposing topics");
		}
		if (!Boolean.TRUE.equals(CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType).get("email"))) {
			throw new IllegalStateException("Email is disabled for this owner");
		}
		if (CollaborationJobUtils.anyRunning(ownerId, ownerType)) {
			throw new IllegalStateException("Wait for the current mailbox job to finish before proposing topics");
		}
		Map<String, Object> window = CollaborationDbUtils.queryOne(CollaborationDbUtils.page(
				"SELECT PARAMS_JSON, STARTED_AT, FINISHED_AT FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND KIND = ? AND STATUS = ? ORDER BY STARTED_AT DESC", 1, 0), rs -> {
			Map<String, Object> row = new HashMap<>();
			row.put("params", CollaborationDbUtils.parseMap(rs.getString(1)));
			row.put("start", rs.getTimestamp(2));
			row.put("end", rs.getTimestamp(3));
			return row;
		}, ownerId, ownerType, BrainMailImport.KIND, CollaborationJobUtils.DONE);
		if (window == null || !(window.get("params") instanceof Map<?, ?> params)
				|| !(params.get("days") instanceof Number days) || days.intValue() < 1 || days.intValue() > 180
				|| !(window.get("start") instanceof Timestamp start) || !(window.get("end") instanceof Timestamp end)) {
			throw new IllegalStateException("A completed mailbox import with a known window is required");
		}
		// job times are stored as UTC wall time (CollaborationDbUtils.now), not JVM local time
		Instant since = start.toLocalDateTime().toInstant(ZoneOffset.UTC).minus(Duration.ofDays(days.intValue()));
		Map<String, String> messages = new LinkedHashMap<>();
		Set<String> permittedThreads = new HashSet<>();
		CollaborationDbUtils.query("SELECT THREAD_ID FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND SOURCE = ? AND (MUTED IS NULL OR MUTED = ?)", rs -> permittedThreads.add(rs.getString(1)),
				ownerId, ownerType, "email", false);
		CollaborationDbUtils.query("SELECT MESSAGE_KEY, THREAD_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND DECISION = ? AND RECEIVED_AT >= ? AND RECEIVED_AT <= ?", rs -> {
			if (permittedThreads.contains(rs.getString(2))) {
				messages.put(rs.getString(1), rs.getString(2));
			}
			return null;
		}, ownerId, ownerType, BrainRulesGate.INGESTED, Timestamp.valueOf(LocalDateTime.ofInstant(since, ZoneOffset.UTC)), end);
		Map<String, String> people = new HashMap<>();
		emails.forEach((id, email) -> { if (email != null) people.put(email, id); });
		CollaborationDbUtils.query("SELECT PERSON_ID, VALUE_NORM FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? ORDER BY PERSON_ID DESC", rs -> people.put(rs.getString(2), rs.getString(1)), ownerId, ownerType);
		Map<String, Set<String>> included = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND (INCLUDED IS NULL OR INCLUDED = ?)", rs -> included
						.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2)), ownerId, ownerType, true);
		Map<String, List<String>> topicIds = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> topicIds.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(rs.getString(2)), ownerId, ownerType);
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		Set<String> vipEmails = new HashSet<>();
		vips.forEach(p -> { if (emails.get(p) != null) vipEmails.add(emails.get(p)); });
		Map<String, List<BrainTopicStructure.Message>> byThread = new LinkedHashMap<>();
		Set<String> sentHistory = new HashSet<>();
		Set<String> seen = new HashSet<>();
		BrainMailHeaderSource source = BrainMailHeaderSource.current();
		try {
			for (String folder : BrainMailHeaderSource.FOLDERS) {
				List<Map<String, Object>> headers = source.topicHeaders(user, folder, since, MAX_PER_FOLDER + 1);
				if (headers.size() > MAX_PER_FOLDER) {
					throw new IllegalStateException("The mailbox window exceeds the header limit; import a shorter window");
				}
				for (Map<String, Object> header : headers) {
					String messageId = BrainMailImport.messageId(header);
					if (messageId == null) {
						continue;
					}
					String key = CollaborationDbUtils.deterministicId(ownerId, ownerType, "email", messageId);
					String thread = messages.get(key);
					if (thread == null || !seen.add(key)) {
						continue;
					}
					String from = permitted(address(header.get("from")), thread, header, people, emails, included,
							topicIds, rules);
					if (from == null || BrainRulesGate.keywordRule(rules, (String) header.get("subject"), null) != null) {
						continue;
					}
					List<String> to = recipients(header.get("toRecipients"), thread, header, people, emails, included, topicIds, rules);
					List<String> cc = recipients(header.get("ccRecipients"), thread, header, people, emails, included, topicIds, rules);
					if (emails.get(self).equals(from)) {
						sentHistory.addAll(to);
						sentHistory.addAll(cc);
					}
					if (eligible.contains(thread)) {
						byThread.computeIfAbsent(thread, k -> new ArrayList<>()).add(new BrainTopicStructure.Message(
								(String) header.get("subject"), (String) header.get("receivedDateTime"), from, to, cc, bulk(header)));
					}
				}
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Mailbox headers could not be refreshed; retry after reconnecting Microsoft");
		}
		// mail archived, deleted or moved since the import is skipped and counted, not a failure
		long missing = messages.keySet().stream().filter(k -> !seen.contains(k)).count();
		notes.put("missingHeaders", missing);
		if (!messages.isEmpty() && missing == messages.size()) {
			throw new IllegalStateException("None of the imported mail is in the inbox or sent items any more; import again");
		}
		List<BrainTopicStructure.MailThread> input = byThread.entrySet().stream()
				.map(e -> new BrainTopicStructure.MailThread(e.getKey(), e.getValue())).toList();
		BrainTopicStructure.Settings settings = settings(ownerId, ownerType);
		notes.put("wide", settings.wide());
		return BrainTopicStructure.prepare(input, emails.get(self), ownOrg::isMine, vipEmails, settings, sentHistory);
	}

	private static String permitted(String address, String thread, Map<String, Object> header, Map<String, String> people,
			Map<String, String> emails, Map<String, Set<String>> included, Map<String, List<String>> topicIds,
			List<BrainRulesGate.Rule> rules) {
		String person = people.get(address);
		if (address == null || person == null || !included.getOrDefault(thread, Set.of()).contains(person)
				|| BrainRulesGate.neverRule(rules, address, person, (String) header.get("parentFolderId")) != null
				|| BrainRulesGate.exclusionRule(rules, topicIds.getOrDefault(thread, List.of()), "email", person) != null) {
			return null;
		}
		return emails.get(person);
	}

	private static List<String> recipients(Object value, String thread, Map<String, Object> header,
			Map<String, String> people, Map<String, String> emails, Map<String, Set<String>> included,
			Map<String, List<String>> topicIds, List<BrainRulesGate.Rule> rules) {
		List<String> out = new ArrayList<>();
		if (value instanceof List<?> list) {
			for (Object item : list) {
				String address = permitted(address(item), thread, header, people, emails, included, topicIds, rules);
				if (address != null) {
					out.add(address);
				}
			}
		}
		return out;
	}

	private static String address(Object value) {
		return value instanceof Map<?, ?> p && p.get("emailAddress") instanceof Map<?, ?> email
				&& email.get("address") instanceof String a ? a.trim().toLowerCase(Locale.ROOT) : null;
	}

	private static boolean bulk(Map<String, Object> header) {
		if ("other".equals(header.get("inferenceClassification"))) {
			return true;
		}
		if (header.get("internetMessageHeaders") instanceof List<?> headers) {
			return headers.stream().anyMatch(h -> h instanceof Map<?, ?> row
					&& "list-unsubscribe".equalsIgnoreCase(String.valueOf(row.get("name"))));
		}
		return false;
	}
}
