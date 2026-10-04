package prerna.collaboration;

import static prerna.collaboration.BrainTopicReviewEvidence.map;
import static prerna.collaboration.BrainTopicReviewEvidence.maps;
import static prerna.collaboration.BrainTopicReviewEvidence.strings;
import static prerna.collaboration.BrainTopicReviewOperations.text;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Reviewed flat combinations. A proposal is a draft change; saved references move only on final apply. */
final class BrainTopicReviewStructure {

	private BrainTopicReviewStructure() {
	}

	static List<Map<String, Object>> groups(Object input) {
		if (!(input instanceof List<?> raw) || raw.isEmpty() || raw.size() > 100) {
			throw new IllegalArgumentException("Review between 1 and 100 topic groups");
		}
		List<Map<String, Object>> result = new ArrayList<>();
		Set<String> seen = new LinkedHashSet<>();
		for (Object item : raw) {
			if (!(item instanceof Map<?, ?>)) throw new IllegalArgumentException("Each proposed group needs a profile");
			Map<String, Object> group = map(item);
			List<String> keys = strings(group.get("topicKeys"));
			if (!(group.get("topicKeys") instanceof List<?> list) || list.size() != keys.size()
					|| keys.isEmpty() || keys.size() > 100) throw new IllegalArgumentException("Choose the topics contributing to this group");
			for (String key : keys) {
				text(key, "Topic key", 100);
				if (!seen.add(key)) throw new IllegalArgumentException("A topic can contribute to only one group in a proposal");
			}
			if (seen.size() > 100) throw new IllegalArgumentException("Review at most 100 contributing topics");
			String target = text(group.get("targetKey"), "Retained topic key", 100);
			if (!keys.contains(target)) throw new IllegalArgumentException("The retained topic must be one of its contributing topics");
			String name = text(group.get("name"), "Topic name", 255).trim();
			String about = optional(group.get("description"), "Topic description", 12000);
			String clues = optional(group.get("terms"), "Topic clues", 4000);
			BrainTopicReviewProfiles.terms(clues);
			result.add(Map.of("topicKeys", keys, "targetKey", target, "name", name,
					"description", about, "terms", clues));
		}
		return result;
	}

	/** Hash exact saved impact and the profiles being edited; bodies never cross the UI/model boundary. */
	static Map<String, Object> preview(Connection conn, String ownerId, String ownerType,
			Map<String, Object> review, List<Map<String, Object>> groups) throws SQLException {
		Map<String, Object> draft = map(review.get("draft"));
		Set<String> lockedThreads = new LinkedHashSet<>();
		for (Map<String, Object> group : groups) for (String key : strings(group.get("topicKeys"))) {
			lockedThreads.addAll(BrainTopicReviewEvidence.ids(ownerId, ownerType, requireKept(review, key), review));
		}
		if (lockedThreads.size() > BrainTopicReviewEvidence.MAX_THREADS) throw new IllegalArgumentException("Review combinations for at most 1,000 conversations at a time");
		for (String thread : lockedThreads.stream().sorted().toList()) BrainThreadTopicDecisions.lockThread(ownerId, ownerType, thread);
		List<Map<String, Object>> impact = new ArrayList<>();
		List<Object> scope = new ArrayList<>();
		scope.add(BrainRulesGate.activeRules(ownerId, ownerType));
		scope.add(CollaborationDbUtils.query("SELECT SOURCE, ENABLED FROM SOURCE_CONNECTION WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY SOURCE",
				rs -> List.of(rs.getString(1), Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "ENABLED"))), ownerId, ownerType));
		int before = (int) maps(draft.get("topics")).stream().filter(topic -> Boolean.TRUE.equals(topic.get("keep"))).count();
		int removed = 0;
		for (Map<String, Object> group : groups) {
			List<String> keys = strings(group.get("topicKeys"));
			List<Map<String, Object>> topics = keys.stream().map(key -> requireKept(review, key)).toList();
			if (keys.size() > 1 && maps(draft.get("topics")).stream().anyMatch(topic -> keys.contains(topic.get("mergedIntoKey")))) {
				throw new IllegalArgumentException("Apply or undo the earlier combination before combining that retained topic again");
			}
			Map<String, Object> target = requireKept(review, (String) group.get("targetKey"));
			String targetId = BrainTopicReviewProfiles.id(target);
			Set<String> allThreads = new LinkedHashSet<>();
			List<Map<String, Object>> contributing = new ArrayList<>();
			Map<String, Integer> counts = new LinkedHashMap<>();
			for (String field : List.of("linkedConversations", "workItems", "steps", "people", "notes", "rules")) counts.put(field, 0);
			List<Map<String, Object>> pending = maps(draft.get("corrections")).stream().filter(row -> keys.contains(row.get("topicKey"))).toList();
			counts.put("pendingRelationships", pending.size());
			counts.put("positiveOverExclusion", preservedPositiveThreads(ownerId, ownerType, topics, pending).size());
			scope.add(pending);
			boolean canCombine = true;
			for (Map<String, Object> topic : topics) {
				Set<String> ids = BrainTopicReviewEvidence.ids(ownerId, ownerType, topic, review);
				allThreads.addAll(ids);
				if (allThreads.size() > BrainTopicReviewEvidence.MAX_THREADS) throw new IllegalArgumentException("This combination exceeds the 1,000-conversation review window. Review a smaller scope.");
				contributing.add(Map.of("key", topic.get("key"), "name", Objects.toString(topic.get("name"), "New topic"),
						"examples", ids.size(), "accepted", Boolean.TRUE.equals(topic.get("accepted"))));
				scope.add(topic);
				String id = BrainTopicReviewProfiles.id(topic);
				if (id != null && keys.size() > 1) {
					if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC tt JOIN BRAIN_THREAD t "
							+ "ON t.OWNER_ID = tt.OWNER_ID AND t.OWNER_TYPE = tt.OWNER_TYPE AND t.THREAD_ID = tt.THREAD_ID "
							+ "WHERE tt.OWNER_ID = ? AND tt.OWNER_TYPE = ? AND tt.TOPIC_ID = ? AND t.SOURCE <> ?", ownerId, ownerType, id, "email")) canCombine = false;
				}
				if (id == null || topic.get("key").equals(group.get("targetKey"))) continue;
				BrainTopicChangeUtils.Snapshot saved = BrainTopicChangeUtils.capture(conn, ownerId, ownerType, id, targetId);
				scope.add(List.of(saved.before, saved.links, saved.mergeCandidates));
				counts.merge("linkedConversations", count(ownerId, ownerType, "BRAIN_THREAD_TOPIC", "TOPIC_ID", id), Integer::sum);
				counts.merge("workItems", count(ownerId, ownerType, "WORK_ITEM", "LINK_TOPIC_ID", id), Integer::sum);
				counts.merge("steps", count(ownerId, ownerType, "WORK_THREAD_STEP", "LINK_TOPIC_ID", id), Integer::sum);
				for (String[] table : new String[][] {{"BRAIN_TOPIC_PERSON", "people"}, {"BRAIN_TOPIC_NOTE", "notes"}, {"BRAIN_RULE", "rules"}}) {
					counts.merge(table[1], count(ownerId, ownerType, table[0], "TOPIC_ID", id), Integer::sum);
				}
			}
			for (String thread : allThreads.stream().sorted().toList()) {
				scope.add(List.of(thread, BrainTopicReviewEvidence.version(ownerId, ownerType, thread)));
			}
			Map<String, Object> row = new LinkedHashMap<>(group);
			row.put("contributing", contributing);
			row.put("examples", allThreads.size());
			row.put("impact", counts);
			row.put("canApply", canCombine);
			row.put("reason", canCombine ? "" : "This group has saved Teams or calendar links. Exchange-aware combination is needed before moving those links.");
			impact.add(row);
			removed += keys.size() - 1;
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("reviewId", review.get("id"));
		result.put("revision", review.get("revision"));
		result.put("groups", impact);
		result.put("beforeCount", before);
		result.put("afterCount", before - removed);
		result.put("scopeVersion", BrainTopicReviewProfiles.hash(List.of(groups, scope)));
		return result;
	}

	/** Combine provisional examples and pending choices, preserving the chosen profile identity. */
	static List<Map<String, Object>> organize(String ownerId, String ownerType, Map<String, Object> review,
			List<Map<String, Object>> groups, String scopeVersion) {
		Map<String, Object> draft = map(review.get("draft"));
		List<Map<String, Object>> topics = new ArrayList<>(maps(draft.get("topics")));
		List<Map<String, Object>> patches = new ArrayList<>();
		List<Map<String, Object>> combinations = new ArrayList<>(maps(draft.get("combinations")));
		List<Map<String, Object>> previewTopics = topics.stream().filter(topic -> groups.stream()
				.anyMatch(group -> strings(group.get("topicKeys")).contains(topic.get("key"))))
				.map(LinkedHashMap<String, Object>::new).map(topic -> (Map<String, Object>) topic).toList();
		List<Map<String, Object>> previewCorrections = maps(draft.get("corrections"));
		Map<String, String> aliases = new LinkedHashMap<>();
		for (Map<String, Object> group : groups) {
			String targetKey = (String) group.get("targetKey");
			Map<String, Object> target = find(topics, targetKey);
			Map<String, Object> targetBefore = new LinkedHashMap<>(target);
			Set<String> threadIds = new LinkedHashSet<>(strings(target.get("threadIds")));
			Map<String, Map<String, Object>> people = new LinkedHashMap<>();
			maps(target.get("people")).forEach(person -> people.put((String) person.get("id"), person));
			Set<String> removedPeople = new LinkedHashSet<>(strings(target.get("removedPeople")));
			Set<String> subjects = new LinkedHashSet<>(strings(target.get("sampleSubjects")));
			Set<String> domains = new LinkedHashSet<>(strings(target.get("domains")));
			for (String sourceKey : strings(group.get("topicKeys"))) {
				if (targetKey.equals(sourceKey)) continue;
				Map<String, Object> source = find(topics, sourceKey);
				Map<String, Object> sourceBefore = new LinkedHashMap<>(source);
				threadIds.addAll(BrainTopicReviewEvidence.ids(ownerId, ownerType, source, review));
				for (Map<String, Object> person : maps(source.get("people"))) {
					String id = (String) person.get("id");
					if (!people.containsKey(id)) {
						people.put(id, person);
						if (strings(source.get("removedPeople")).contains(id)) removedPeople.add(id);
					}
				}
				subjects.addAll(strings(source.get("sampleSubjects")));
				domains.addAll(strings(source.get("domains")));
				source.put("keep", false);
				source.put("mergedIntoKey", targetKey);
				aliases.put(sourceKey, targetKey);
				patches.add(patch(sourceBefore, source));
			}
			target.put("name", group.get("name"));
			target.put("description", group.get("description"));
			target.put("terms", group.get("terms"));
			target.put("threadIds", new ArrayList<>(threadIds));
			target.put("people", new ArrayList<>(people.values()));
			target.put("removedPeople", new ArrayList<>(removedPeople));
			target.put("sampleSubjects", subjects.stream().limit(6).toList());
			target.put("domains", new ArrayList<>(domains));
			patches.add(patch(targetBefore, target));
		}
		if (!aliases.isEmpty()) combinations.add(Map.of("groups", groups, "scopeVersion", scopeVersion,
				"previewTopics", previewTopics, "previewCorrections", previewCorrections));
		draft.put("combinations", combinations);
		draft.put("topics", topics);
		draft.put("corrections", remap(ownerId, ownerType, maps(draft.get("corrections")), aliases, groups, previewTopics));
		review.put("draft", draft);
		return patches;
	}

	/** Recheck saved impact before profile changes. Draft edits after a proposal do not invalidate its saved-data basis. */
	static void validateApply(Connection conn, String ownerId, String ownerType, Map<String, Object> review) throws SQLException {
		Map<String, Object> draft = map(review.get("draft"));
		for (Map<String, Object> combination : maps(draft.get("combinations"))) {
			List<Map<String, Object>> original = maps(combination.get("previewTopics"));
			List<Map<String, Object>> topics = new ArrayList<>(maps(draft.get("topics")));
			topics.replaceAll(topic -> original.stream().filter(before -> Objects.equals(before.get("key"), topic.get("key"))).findFirst().orElse(topic));
			Map<String, Object> oldDraft = new LinkedHashMap<>(draft);
			oldDraft.put("topics", topics);
			oldDraft.put("corrections", maps(combination.get("previewCorrections")));
			Map<String, Object> oldReview = new LinkedHashMap<>(review);
			oldReview.put("draft", oldDraft);
			Map<String, Object> checked = preview(conn, ownerId, ownerType, oldReview, maps(combination.get("groups")));
			if (!Objects.equals(checked.get("scopeVersion"), combination.get("scopeVersion"))
					|| maps(checked.get("groups")).stream().anyMatch(group -> !Boolean.TRUE.equals(group.get("canApply")))) {
				throw new IllegalArgumentException("The saved relationships or references in a combination changed. Undo that grouping and refresh its preview before applying setup.");
			}
		}
	}

	/** Restore structural metadata; subsequent owner profile edits are retained. */
	static void undo(Map<String, Object> draft, Map<String, Object> change) {
		List<Map<String, Object>> topics = new ArrayList<>(maps(draft.get("topics")));
		for (Map<String, Object> patch : maps(change.get("topicPatches"))) {
			Map<String, Object> topic = find(topics, (String) patch.get("key"));
			Map<String, Object> before = map(patch.get("before"));
			Map<String, Object> after = map(patch.get("after"));
			Set<String> changedFields = new LinkedHashSet<>(after.keySet());
			changedFields.addAll(before.keySet());
			for (String field : changedFields) {
				if (Objects.equals(before.get(field), after.get(field))) continue;
				if (Objects.equals(topic.get(field), after.get(field))) {
					if (before.get(field) == null) topic.remove(field); else topic.put(field, before.get(field));
				} else if (!Set.of("name", "description", "short", "terms", "removedPeople").contains(field)) {
					throw new IllegalArgumentException("This grouping changed since its preview; Undo would replace a newer choice");
				}
			}
		}
		draft.put("topics", topics);
		draft.put("combinations", maps(map(change.get("before")).get("combinations")));
	}

	/** Final application transfers the selected scopes using the existing transactional merge service. */
	static List<Map<String, Object>> apply(prerna.auth.User user, Map<String, Object> draft) {
		List<Map<String, Object>> topics = maps(draft.get("topics"));
		List<Map<String, Object>> receipt = new ArrayList<>();
		for (Map<String, Object> source : topics) {
			if (!(source.get("mergedIntoKey") instanceof String targetKey) || Boolean.TRUE.equals(source.get("mergeApplied"))) continue;
			Map<String, Object> target = find(topics, targetKey);
			String sourceId = BrainTopicReviewProfiles.id(source);
			String targetId = BrainTopicReviewProfiles.id(target);
			if (targetId == null) throw new IllegalArgumentException("The retained profile was not saved");
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("sourceKey", source.get("key"));
			row.put("targetKey", targetKey);
			row.put("targetId", targetId);
			row.put("sourceId", sourceId);
			if (sourceId != null) {
				row.putAll(BrainTopicUtils.mergeTopics(user, sourceId, targetId));
				var owner = CollaborationDbUtils.ownerOf(user);
				for (String table : List.of("BRAIN_TOPIC", "BRAIN_THREAD_TOPIC", "BRAIN_THREAD_TOPIC_REJECTION", "BRAIN_TOPIC_PERSON", "BRAIN_TOPIC_NOTE", "BRAIN_RULE", "WORK_ITEM", "WORK_THREAD_STEP")) {
					String field = table.startsWith("WORK_") ? "LINK_TOPIC_ID" : "TOPIC_ID";
					if (count(owner.getValue0(), owner.getValue1(), table, field, sourceId) != 0) throw new IllegalStateException("The topic combination's saved references could not be verified");
				}
			}
			source.put("id", null);
			source.put("mergeApplied", true);
			receipt.add(row);
		}
		draft.put("topics", topics);
		draft.put("combinations", List.of());
		return receipt;
	}

	static Map<String, Object> requireKept(Map<String, Object> review, String key) {
		Map<String, Object> topic = BrainTopicReviewEvidence.topic(review, key);
		if (!Boolean.TRUE.equals(topic.get("keep")) || topic.get("mergedIntoKey") != null) {
			throw new IllegalArgumentException("Keep a topic before using it in a combination");
		}
		return topic;
	}

	private static List<Map<String, Object>> remap(String ownerId, String ownerType, List<Map<String, Object>> rows,
			Map<String, String> aliases, List<Map<String, Object>> groups, List<Map<String, Object>> originalTopics) {
		Map<String, Map<String, Object>> combined = new LinkedHashMap<>();
		for (Map<String, Object> row : rows) {
			Map<String, Object> mapped = new LinkedHashMap<>(row);
			String key = aliases.getOrDefault((String) row.get("topicKey"), (String) row.get("topicKey"));
			mapped.put("topicKey", key);
			String id = row.get("threadId") + ":" + key;
			Map<String, Object> current = combined.get(id);
			if (current != null && "include".equals(current.get("state"))) {
				if ("include".equals(mapped.get("state")) && Boolean.TRUE.equals(mapped.get("primary"))) current.put("primary", true);
				continue;
			}
			combined.put(id, mapped);
		}
		for (Map<String, Object> group : groups) {
			List<String> keys = strings(group.get("topicKeys"));
			List<Map<String, Object>> contributors = originalTopics.stream().filter(topic -> keys.contains(topic.get("key"))).toList();
			for (String thread : preservedPositiveThreads(ownerId, ownerType, contributors, rows)) {
				String id = thread + ":" + group.get("targetKey");
				Map<String, Object> correction = combined.get(id);
				if (correction != null && "exclude".equals(correction.get("state"))) combined.remove(id);
			}
		}
		return new ArrayList<>(combined.values());
	}

	/** A negative example of one narrow scope cannot erase a surviving positive link to another contributor. */
	private static Set<String> preservedPositiveThreads(String ownerId, String ownerType,
			List<Map<String, Object>> topics, List<Map<String, Object>> corrections) {
		Set<String> keys = new LinkedHashSet<>();
		topics.forEach(topic -> keys.add((String) topic.get("key")));
		List<Map<String, Object>> relevant = corrections.stream().filter(row -> keys.contains(row.get("topicKey"))).toList();
		Set<String> preserved = new LinkedHashSet<>();
		for (String thread : relevant.stream().filter(row -> "exclude".equals(row.get("state"))).map(row -> (String) row.get("threadId")).distinct().toList()) {
			List<Map<String, Object>> links = BrainThreadUtils.getLinks(ownerId, ownerType, thread);
			boolean positive = topics.stream().anyMatch(topic -> {
				Map<String, Object> choice = relevant.stream().filter(row -> thread.equals(row.get("threadId"))
						&& Objects.equals(row.get("topicKey"), topic.get("key"))).findFirst().orElse(null);
				return choice == null ? links.stream().anyMatch(link -> Objects.equals(link.get("topicId"), topic.get("id")))
						: "include".equals(choice.get("state"));
			});
			if (positive) preserved.add(thread);
		}
		return preserved;
	}

	private static Map<String, Object> patch(Map<String, Object> before, Map<String, Object> after) {
		Map<String, Object> oldFields = new LinkedHashMap<>(), newFields = new LinkedHashMap<>();
		for (String key : after.keySet()) if (!Objects.equals(before.get(key), after.get(key))) {
			oldFields.put(key, before.get(key)); newFields.put(key, after.get(key));
		}
		return Map.of("key", after.get("key"), "before", oldFields, "after", newFields);
	}

	private static Map<String, Object> find(List<Map<String, Object>> topics, String key) {
		return topics.stream().filter(topic -> Objects.equals(key, topic.get("key"))).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Draft topic not found"));
	}

	private static int count(String ownerId, String ownerType, String table, String field, String id) {
		return CollaborationDbUtils.count("SELECT COUNT(*) FROM " + table + " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND " + field + " = ?", ownerId, ownerType, id);
	}

	private static String optional(Object value, String field, int max) {
		if (value == null) return "";
		if (!(value instanceof String text) || text.length() > max) throw new IllegalArgumentException(field + " must fit in " + max + " characters");
		return text;
	}
}
