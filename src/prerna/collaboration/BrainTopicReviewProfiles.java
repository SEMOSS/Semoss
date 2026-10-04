package prerna.collaboration;

import static prerna.collaboration.BrainTopicReviewEvidence.maps;
import static prerna.collaboration.BrainTopicReviewEvidence.strings;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/** Profile clues and external-change checks shared by manual and assisted onboarding. */
final class BrainTopicReviewProfiles {

	private BrainTopicReviewProfiles() {
	}

	/** Existing reviews are upgraded under the review lock without replacing owner edits. */
	static void initialize(String ownerId, String ownerType, Map<String, Object> draft) {
		List<Map<String, Object>> topics = new ArrayList<>(maps(draft.get("topics")));
		for (Map<String, Object> topic : topics) {
			String id = id(topic);
			if (id != null) {
				Map<String, Object> saved = saved(ownerId, ownerType, id);
				topic.putIfAbsent("terms", String.join("\n", strings(saved.get("keywords"))));
				topic.putIfAbsent("profileBasis", saved.isEmpty() ? hash(List.of("missing saved topic", id)) : version(saved));
			} else {
				topic.putIfAbsent("terms", "");
			}
		}
		draft.put("topics", topics);
		draft.putIfAbsent("guidance", "");
		draft.putIfAbsent("granularity", "broad");
	}

	/** A saved profile or membership changed outside this draft; preserve it and request reconciliation. */
	static void validate(String ownerId, String ownerType, Map<String, Object> draft) {
		if (!conflicts(ownerId, ownerType, draft).isEmpty()) throw new IllegalArgumentException("A saved topic profile or its people changed elsewhere. Reload the saved review and compare those changes before applying this setup.");
	}

	/** Read-only conflict summaries. Deleted profiles leave the draft recoverable as well. */
	static List<Map<String, Object>> conflicts(String ownerId, String ownerType, Map<String, Object> draft) {
		List<Map<String, Object>> conflicts = new ArrayList<>();
		for (Map<String, Object> topic : maps(draft.get("topics"))) {
			String id = id(topic);
			if (id == null || topic.get("profileBasis") == null) continue;
			Map<String, Object> saved = saved(ownerId, ownerType, id);
			String version = version(saved);
			if (Objects.equals(topic.get("profileBasis"), version)) continue;
			String key = (String) topic.get("key");
			boolean pending = maps(draft.get("corrections")).stream().anyMatch(row -> key.equals(row.get("topicKey")))
					|| maps(draft.get("combinations")).stream().flatMap(row -> maps(row.get("groups")).stream())
						.anyMatch(group -> strings(group.get("topicKeys")).contains(key));
			Map<String, Object> profile = new LinkedHashMap<>();
			for (String field : List.of("name", "short", "description", "kind", "status")) profile.put(field, Objects.toString(saved.get(field), ""));
			profile.put("terms", String.join("\n", strings(saved.get("keywords"))));
			profile.put("people", people(ownerId, ownerType, saved));
			conflicts.add(Map.of("topicKey", key, "profileVersion", version, "exists", !saved.isEmpty(),
					"savedProfile", profile, "canReconcile", !pending,
					"reason", pending ? "Undo the pending grouping or conversation choices for this topic before comparing its saved profile." : ""));
		}
		return conflicts;
	}

	/** A version-bound owner choice rebases the draft only; final apply still owns real profile writes. */
	static Map<String, Object> reconcile(String ownerId, String ownerType, Map<String, Object> draft,
			String key, String expectedVersion, String choice) {
		Map<String, Object> conflict = conflicts(ownerId, ownerType, draft).stream().filter(row -> key.equals(row.get("topicKey"))).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("This topic no longer has a saved-profile conflict. Reload the saved review."));
		if (!Objects.equals(expectedVersion, conflict.get("profileVersion"))) throw new IllegalArgumentException("The saved topic changed again. Reload its comparison before choosing a profile.");
		if (!Boolean.TRUE.equals(conflict.get("canReconcile"))) throw new IllegalArgumentException((String) conflict.get("reason"));
		List<Map<String, Object>> topics = new ArrayList<>(maps(draft.get("topics")));
		Map<String, Object> topic = topics.stream().filter(row -> key.equals(row.get("key"))).findFirst().orElseThrow();
		Map<String, Object> before = new LinkedHashMap<>(topic);
		Map<String, Object> saved = saved(ownerId, ownerType, id(topic));
		if (saved.isEmpty()) {
			topic.put("id", null);
			topic.put("accepted", false);
			topic.remove("profileBasis");
			topic.put("appliedRemovedPeople", List.of());
			if ("saved".equals(choice)) topic.put("keep", false);
		} else {
			if ("saved".equals(choice)) {
				for (String field : List.of("name", "short", "description")) topic.put(field, Objects.toString(saved.get(field), ""));
				topic.put("terms", String.join("\n", strings(saved.get("keywords"))));
			}
			// Preserve current saved membership. An explicit draft removal remains a removal.
			List<Map<String, Object>> people = people(ownerId, ownerType, saved);
			Set<String> removed = new java.util.LinkedHashSet<>();
			if ("draft".equals(choice)) removed.addAll(strings(topic.get("removedPeople")));
			people.stream().filter(person -> BrainTopicUtils.REMOVED.equals(person.get("state"))).forEach(person -> removed.add((String) person.get("id")));
			Set<String> known = new java.util.LinkedHashSet<>();
			people.forEach(person -> known.add((String) person.get("id")));
			removed.retainAll(known);
			topic.put("people", people);
			topic.put("removedPeople", new ArrayList<>(removed));
			topic.put("appliedRemovedPeople", people.stream().filter(person -> BrainTopicUtils.REMOVED.equals(person.get("state"))).map(person -> person.get("id")).toList());
			topic.put("accepted", !BrainTopicUtils.SUGGESTED.equals(saved.get("status")));
			topic.put("profileBasis", version(saved));
		}
		draft.put("topics", topics);
		return Map.of("key", key, "before", before, "after", new LinkedHashMap<>(topic));
	}

	private static List<Map<String, Object>> people(String ownerId, String ownerType, Map<String, Object> saved) {
		return maps(saved.get("people")).stream().map(person -> {
			String id = (String) person.get("personId");
			Map<String, Object> detail = BrainPeopleUtils.getPerson(ownerId, ownerType, id);
			return Map.<String, Object>of("id", id, "name", Objects.toString(detail == null ? null : detail.get("name"), id),
					"state", Objects.toString(person.get("state"), BrainTopicUtils.SUGGESTED));
		}).toList();
	}

	private static Map<String, Object> saved(String ownerId, String ownerType, String id) {
		Map<String, Object> profile = CollaborationDbUtils.queryOne("SELECT TOPIC_ID, NAME, SHORT_NAME, DESCRIPTION, KIND, ACCOUNT_ID, STATUS, KEYWORDS_JSON, CALENDAR_SERIES_JSON "
				+ "FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", rs -> {
			Map<String, Object> row = new LinkedHashMap<>();
			for (String[] field : new String[][] {{"id", "TOPIC_ID"}, {"name", "NAME"}, {"short", "SHORT_NAME"}, {"description", "DESCRIPTION"}, {"kind", "KIND"}, {"accountId", "ACCOUNT_ID"}, {"status", "STATUS"}}) {
				row.put(field[0], CollaborationDbUtils.getString(rs, field[1]));
			}
			row.put("keywords", CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "KEYWORDS_JSON")));
			row.put("calendarSeries", CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "CALENDAR_SERIES_JSON")));
			return row;
		}, ownerId, ownerType, id);
		if (profile == null) return Map.of();
		profile.put("people", BrainTopicUtils.getPeople(ownerId, ownerType, id));
		return profile;
	}

	/** Refresh the basis only after successful writes in the same transaction. */
	static void accept(String ownerId, String ownerType, Map<String, Object> topic) {
		String id = id(topic);
		if (id != null) topic.put("profileBasis", version(BrainTopicUtils.getTopic(ownerId, ownerType, id)));
	}

	/** One term per line; clue matching is case-insensitive without rewriting the owner's display text. */
	static List<String> terms(Object input) {
		if (!(input instanceof String text) || text.length() > 4000) {
			throw new IllegalArgumentException("Use at most 4,000 characters of topic clues");
		}
		Map<String, String> unique = new LinkedHashMap<>();
		for (String line : text.split("\\R")) {
			String term = line.trim();
			if (term.length() > 200) throw new IllegalArgumentException("Each topic clue must fit in 200 characters");
			if (!term.isEmpty()) unique.putIfAbsent(term.toLowerCase(Locale.ROOT), term);
		}
		if (unique.size() > 50) throw new IllegalArgumentException("Use at most 50 topic clues");
		return new ArrayList<>(unique.values());
	}

	/** Serialize topic mutations against an existing review across server processes. No review is created here. */
	static <T> T serialized(String ownerId, String ownerType, Supplier<T> work) {
		List<T> result = new ArrayList<>(1);
		CollaborationDbUtils.batch(conn -> {
			lock(ownerId, ownerType);
			result.add(work.get());
		});
		return result.get(0);
	}

	static void lock(String ownerId, String ownerType) {
		CollaborationDbUtils.queryOne("SELECT REVIEW_ID FROM BRAIN_TOPIC_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? FOR UPDATE",
				rs -> rs.getString(1), ownerId, ownerType);
	}

	static String hash(Object value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(CollaborationDbUtils.toJson(canonical(value)).getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException impossible) {
			throw new IllegalStateException("SHA-256 is unavailable", impossible);
		}
	}

	private static Object canonical(Object value) {
		if (value instanceof Map<?, ?> values) {
			Map<String, Object> sorted = new TreeMap<>();
			values.forEach((key, item) -> sorted.put(Objects.toString(key), canonical(item)));
			return sorted;
		}
		if (value instanceof List<?> values) return values.stream().map(BrainTopicReviewProfiles::canonical).toList();
		return value;
	}

	static String id(Map<String, Object> topic) {
		return topic.get("id") instanceof String id && !id.isBlank() ? id : null;
	}

	private static String version(Map<String, Object> saved) {
		Map<String, Object> semantic = new TreeMap<>();
		for (String field : List.of("id", "name", "short", "description", "kind", "accountId", "keywords", "calendarSeries")) {
			semantic.put(field, saved.get(field));
		}
		// An automatic dormant transition does not alter the owner's organizing profile.
		semantic.put("status", BrainTopicUtils.DORMANT.equals(saved.get("status")) ? BrainTopicUtils.ACTIVE : saved.get("status"));
		semantic.put("people", maps(saved.get("people")).stream().map(person -> {
			Map<String, Object> row = new TreeMap<>();
			for (String field : List.of("personId", "state", "role", "origin")) row.put(field, person.get(field));
			return row;
		}).toList());
		return hash(semantic);
	}
}
