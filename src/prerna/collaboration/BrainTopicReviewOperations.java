package prerna.collaboration;

import static prerna.collaboration.BrainTopicReviewEvidence.map;
import static prerna.collaboration.BrainTopicReviewEvidence.maps;
import static prerna.collaboration.BrainTopicReviewEvidence.strings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import prerna.auth.User;

/** Validated reversible draft operations, shared by direct controls and later assistant proposals. */
final class BrainTopicReviewOperations {
	private static final int MAX_CORRECTIONS = 500;
	private static final int KEEP_HISTORY = 20;

	private BrainTopicReviewOperations() {
	}

	/** Validate the operation before checking its retry receipt. Unknown operations never become writes. */
	static Map<String, Object> request(Map<String, Object> input) {
		String type = text(input.get("type"), "Operation", 30);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("type", type);
		if ("undo".equals(type)) {
			out.put("changeId", text(input.get("changeId"), "Change ID", 100));
			return out;
		}
		if (!Set.of("confirm", "reject", "move", "also_link").contains(type)) {
			throw new IllegalArgumentException("Choose confirm, reject, move, also_link or undo");
		}
		out.put("topicKey", text(input.get("topicKey"), "Topic key", 100));
		List<String> ids = strings(input.get("threadIds"));
		if (!(input.get("threadIds") instanceof List<?> raw) || raw.size() != ids.size()
				|| ids.isEmpty() || ids.size() > 50 || new LinkedHashSet<>(ids).size() != ids.size()) {
			throw new IllegalArgumentException("Select between 1 and 50 distinct conversations");
		}
		ids.forEach(id -> text(id, "Thread ID", 50));
		out.put("threadIds", ids);
		Map<String, Object> versions = map(input.get("versions"));
		if (!versions.keySet().equals(new LinkedHashSet<>(ids))) {
			throw new IllegalArgumentException("Every selected conversation needs its current preview version");
		}
		versions.replaceAll((id, version) -> text(version, "Conversation preview version", 50000));
		out.put("versions", versions);
		if (Set.of("move", "also_link").contains(type)) {
			out.put("targetKey", text(input.get("targetKey"), "Destination topic", 100));
		}
		return out;
	}

	static boolean wasApplied(Map<String, Object> draft, String operationId, Map<String, Object> request) {
		for (Map<String, Object> receipt : maps(draft.get("operationReceipts"))) {
			if (operationId.equals(receipt.get("id"))) {
				if (!Objects.equals(receipt.get("request"), request)) {
					throw new IllegalArgumentException("An operation ID cannot be reused for a different change");
				}
				return true;
			}
		}
		return false;
	}

	static void change(String ownerId, String ownerType, Map<String, Object> review, String operationId,
			Map<String, Object> request) {
		Map<String, Object> draft = map(review.get("draft"));
		String type = (String) request.get("type");
		List<Map<String, Object>> history = new ArrayList<>(maps(draft.get("history")));
		if ("undo".equals(type)) {
			if (history.isEmpty() || !request.get("changeId").equals(history.get(history.size() - 1).get("id"))) {
				throw new IllegalArgumentException("Only the latest unapplied conversation change can be undone");
			}
			Map<String, Object> latest = history.get(history.size() - 1);
			if (!Objects.equals(latest.get("after"), state(draft))) {
				throw new IllegalArgumentException("Conversation changes advanced; undo would overwrite them");
			}
			Map<String, Object> before = map(latest.get("before"));
			draft.put("corrections", maps(before.get("corrections")));
			draft.put("baseVersions", map(before.get("baseVersions")));
			history.remove(history.size() - 1);
			draft.put("lastChange", "Undid " + latest.get("summary"));
		} else {
			Map<String, Object> source = kept(review, (String) request.get("topicKey"));
			String sourceKey = (String) source.get("key");
			Map<String, Object> target = Set.of("move", "also_link").contains(type)
					? kept(review, (String) request.get("targetKey")) : null;
			if (target != null && sourceKey.equals(target.get("key"))) {
				throw new IllegalArgumentException("Choose a different destination topic");
			}
			Set<String> allowed = BrainTopicReviewEvidence.ids(ownerId, ownerType, source, review);
			List<String> ids = strings(request.get("threadIds"));
			if (!allowed.containsAll(ids)) {
				throw new IllegalArgumentException("Review only conversations shown for this topic");
			}
			Map<String, Object> before = state(draft);
			Map<String, Object> base = map(draft.get("baseVersions"));
			List<Map<String, Object>> corrections = new ArrayList<>(maps(draft.get("corrections")));
			for (String threadId : ids.stream().sorted().toList()) {
				BrainThreadTopicDecisions.lockThread(ownerId, ownerType, threadId);
				BrainTopicReviewEvidence.requireEmail(ownerId, ownerType, threadId);
				String currentVersion = BrainTopicReviewEvidence.version(ownerId, ownerType, threadId);
				if (!currentVersion.equals(map(request.get("versions")).get(threadId))) {
					throw new IllegalArgumentException("This conversation's topic links changed. Refresh its preview before correcting it.");
				}
				base.put(threadId, currentVersion);
				put(corrections, threadId, sourceKey, Set.of("reject", "move").contains(type) ? "exclude" : "include", false);
				if (target != null) {
					put(corrections, threadId, (String) target.get("key"), "include", "move".equals(type));
				}
			}
			if (corrections.size() > MAX_CORRECTIONS) {
				throw new IllegalArgumentException("Apply this batch before reviewing more than " + MAX_CORRECTIONS + " topic relationships");
			}
			draft.put("corrections", corrections);
			draft.put("baseVersions", base);
			String summary = switch (type) {
			case "confirm" -> "Confirmed " + ids.size() + " conversation(s) for " + source.get("name");
			case "reject" -> "Excluded " + ids.size() + " conversation(s) from " + source.get("name");
			case "move" -> "Moved " + ids.size() + " conversation(s) to " + target.get("name");
			default -> "Also linked " + ids.size() + " conversation(s) to " + target.get("name");
			};
			history.add(Map.of("id", operationId, "type", type, "summary", summary, "before", before, "after", state(draft)));
			if (history.size() > KEEP_HISTORY) {
				history.remove(0);
			}
			draft.put("lastChange", summary);
		}
		draft.put("history", history);
		List<Map<String, Object>> receipts = new ArrayList<>(maps(draft.get("operationReceipts")));
		receipts.add(Map.of("id", operationId, "request", request));
		if (receipts.size() > 100) {
			receipts.remove(0);
		}
		draft.put("operationReceipts", receipts);
		draft.put("operationIds", receipts.stream().map(receipt -> receipt.get("id")).toList());
		review.put("draft", draft);
	}

	/** Lock and validate the exact before-state before profile changes or relationship writes. */
	static void validateApply(String ownerId, String ownerType, Map<String, Object> review) {
		Map<String, Object> draft = map(review.get("draft"));
		Map<String, Object> versions = map(draft.get("baseVersions"));
		for (String threadId : versions.keySet().stream().sorted().toList()) {
			BrainThreadTopicDecisions.lockThread(ownerId, ownerType, threadId);
			BrainTopicReviewEvidence.requireEmail(ownerId, ownerType, threadId);
			if (!BrainTopicReviewEvidence.version(ownerId, ownerType, threadId).equals(versions.get(threadId))) {
				throw new IllegalArgumentException("A reviewed conversation changed. Refresh its preview and review its correction before applying setup.");
			}
		}
		for (Map<String, Object> correction : maps(draft.get("corrections"))) {
			kept(review, (String) correction.get("topicKey"));
		}
	}

	/** Apply only the reviewed relationships; task state, rooms and unrelated links are untouched. */
	static List<Map<String, Object>> apply(User user, String ownerId, String ownerType, Map<String, Object> draft,
			List<Map<String, Object>> receipts) {
		Map<String, String> ids = new LinkedHashMap<>();
		receipts.forEach(topic -> ids.put((String) topic.get("key"), (String) topic.get("id")));
		List<Map<String, Object>> corrections = maps(draft.get("corrections"));
		for (Map<String, Object> correction : corrections) {
			if ("exclude".equals(correction.get("state"))) {
				BrainThreadUtils.linkThreadTopic(user, (String) correction.get("threadId"),
						ids.get(correction.get("topicKey")), false, true);
			}
		}
		for (boolean primary : List.of(false, true)) {
			for (Map<String, Object> correction : corrections) {
				if ("include".equals(correction.get("state")) && primary == Boolean.TRUE.equals(correction.get("primary"))) {
					BrainThreadUtils.linkThreadTopic(user, (String) correction.get("threadId"),
							ids.get(correction.get("topicKey")), primary, false);
					CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = ?, RESOLVED_AT = ? WHERE OWNER_ID = ? "
							+ "AND OWNER_TYPE = ? AND KIND = ? AND REF_TYPE = ? AND REF_ID = ? AND STATUS = ?",
							"accepted", CollaborationDbUtils.now(), ownerId, ownerType, BrainReviewUtils.TOPIC_CHOICE,
							"thread", correction.get("threadId"), "open");
				}
			}
		}
		List<Map<String, Object>> results = new ArrayList<>();
		for (String threadId : corrections.stream().map(row -> (String) row.get("threadId")).distinct().toList()) {
			List<Map<String, Object>> links = BrainThreadUtils.getLinks(ownerId, ownerType, threadId);
			String primaryId = links.stream().filter(link -> Boolean.TRUE.equals(link.get("primary")))
					.map(link -> (String) link.get("topicId")).findFirst().orElse(null);
			List<String> removed = corrections.stream().filter(row -> threadId.equals(row.get("threadId"))
					&& "exclude".equals(row.get("state"))).map(row -> ids.get(row.get("topicKey"))).toList();
			for (String table : List.of("WORK_ITEM", "WORK_THREAD_STEP")) {
				List<Object> params = new ArrayList<>();
				params.add(primaryId);
				params.addAll(List.of(ownerId, ownerType, threadId));
				params.addAll(removed);
				CollaborationDbUtils.update("UPDATE " + table + " SET LINK_TOPIC_ID = ? WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND THREAD_ID = ? AND (LINK_TOPIC_ID IS NULL"
						+ (removed.isEmpty() ? "" : " OR LINK_TOPIC_ID IN (" + CollaborationDbUtils.placeholders(removed.size()) + ")")
						+ ")", params.toArray());
			}
			results.add(Map.of("threadId", threadId, "changes", corrections.stream().filter(row -> threadId.equals(row.get("threadId"))).toList(),
					"links", links,
					"rejectedTopicIds", BrainThreadTopicDecisions.rejected(ownerId, ownerType, threadId).stream().sorted().toList()));
		}
		draft.put("corrections", List.of());
		draft.put("baseVersions", Map.of());
		draft.put("history", List.of());
		draft.put("lastChange", "");
		return results;
	}

	private static Map<String, Object> state(Map<String, Object> draft) {
		return Map.of("corrections", new ArrayList<>(maps(draft.get("corrections"))), "baseVersions", map(draft.get("baseVersions")));
	}

	private static Map<String, Object> kept(Map<String, Object> review, String key) {
		Map<String, Object> topic = BrainTopicReviewEvidence.topic(review, key);
		if (!Boolean.TRUE.equals(topic.get("keep"))) {
			throw new IllegalArgumentException("Keep this topic or undo its conversation changes before continuing");
		}
		return topic;
	}

	private static void put(List<Map<String, Object>> corrections, String threadId, String key, String state, boolean primary) {
		if (!primary && "include".equals(state)) {
			primary = corrections.stream().anyMatch(row -> threadId.equals(row.get("threadId"))
					&& key.equals(row.get("topicKey")) && "include".equals(row.get("state"))
					&& Boolean.TRUE.equals(row.get("primary")));
		}
		if (primary) {
			for (int i = 0; i < corrections.size(); i++) {
				Map<String, Object> previous = corrections.get(i);
				if (threadId.equals(previous.get("threadId")) && Boolean.TRUE.equals(previous.get("primary"))) {
					Map<String, Object> secondary = new LinkedHashMap<>(previous);
					secondary.put("primary", false);
					corrections.set(i, secondary);
				}
			}
		}
		corrections.removeIf(row -> threadId.equals(row.get("threadId")) && key.equals(row.get("topicKey")));
		corrections.add(Map.of("threadId", threadId, "topicKey", key, "state", state, "primary", primary));
	}

	static String text(Object value, String field, int max) {
		if (!(value instanceof String text) || text.isBlank() || text.length() > max) {
			throw new IllegalArgumentException(field + " is required and must fit in " + max + " characters");
		}
		return text;
	}
}
