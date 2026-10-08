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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import prerna.auth.User;

/** Owner-scoped, resumable topic setup. Draft writes and apply receipts use optimistic revisions. */
public final class BrainTopicReviewUtils {
	private static final int MAX_TOPICS = 100;
	private static final String COLUMNS = "REVIEW_ID, REVISION, DRAFT_JSON, APPLIED_REVISION, RESULT_JSON, "
			+ "FILING_JOB_ID, UPDATED_AT";

	private BrainTopicReviewUtils() {
	}

	/** Read without generating suggestions or changing topic profiles. */
	public static Map<String, Object> get(User user) {
		var owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> review = read(owner.getValue0(), owner.getValue1(), false);
		if (review != null) {
			Map<String, Object> draft = map(review.get("draft"));
			BrainTopicReviewProfiles.initialize(owner.getValue0(), owner.getValue1(), draft);
			review.put("draft", draft);
		}
		return envelope(user, review);
	}

	/** Optional contextual suggestions. The model runs outside the transaction; no draft or room is changed. */
	public static Map<String, Object> suggestOrganization(User user, String reviewId, int revision) {
		var owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> context = new LinkedHashMap<>();
		CollaborationDbUtils.batch(conn -> {
			Map<String, Object> review = requireReview(owner.getValue0(), owner.getValue1(), reviewId, true);
			requireRevision(review, revision);
			BrainTopicReviewProfiles.validate(owner.getValue0(), owner.getValue1(), map(review.get("draft")));
			context.putAll(BrainTopicReviewAssistant.context(owner.getValue0(), owner.getValue1(), review));
		});
		Map<String, Object> result = new LinkedHashMap<>(BrainTopicReviewAssistant.ask(user, context));
		requireRevision(requireReview(owner.getValue0(), owner.getValue1(), reviewId, false), revision);
		result.put("reviewId", reviewId);
		result.put("revision", revision);
		return result;
	}

	/** Preview the exact impact for manually chosen groups or assistant proposals. */
	public static Map<String, Object> previewOrganization(User user, String reviewId, int revision, Object groups) {
		var owner = CollaborationDbUtils.ownerOf(user);
		List<Map<String, Object>> requested = BrainTopicReviewStructure.groups(groups);
		Map<String, Object> result = new LinkedHashMap<>();
		CollaborationDbUtils.batch(conn -> {
			Map<String, Object> review = requireReview(owner.getValue0(), owner.getValue1(), reviewId, true);
			requireRevision(review, revision);
			BrainTopicReviewProfiles.validate(owner.getValue0(), owner.getValue1(), map(review.get("draft")));
			result.putAll(BrainTopicReviewStructure.preview(conn, owner.getValue0(), owner.getValue1(), review, requested));
		});
		return result;
	}

	/** Read bounded real metadata without applying the suggestion's provisional membership. */
	public static Map<String, Object> evidence(User user, String reviewId, int revision, String topicKey,
			String query, int offset, int limit) {
		var owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> result = new LinkedHashMap<>();
		CollaborationDbUtils.batch(conn -> {
			Map<String, Object> review = requireReview(owner.getValue0(), owner.getValue1(), reviewId, true);
			requireRevision(review, revision);
			result.putAll(BrainTopicReviewEvidence.list(owner.getValue0(), owner.getValue1(), review, topicKey, query, offset, limit));
		});
		return result;
	}

	/** One revision-checked, idempotent draft correction; profiles and real links wait for final apply. */
	public static Map<String, Object> change(User user, String reviewId, int revision, String operationId,
			Map<String, Object> operation) {
		if (revision < 1) {
			throw conflict();
		}
		BrainTopicReviewOperations.text(operationId, "Operation ID", 100);
		Map<String, Object> request = BrainTopicReviewOperations.request(operation);
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock("topic-onboarding", ownerId, ownerType)) {
			CollaborationDbUtils.batch(conn -> {
				Map<String, Object> current = requireReview(ownerId, ownerType, reviewId, true);
				if (BrainTopicReviewOperations.wasApplied(map(current.get("draft")), operationId, request)) {
					if (revision > integer(current.get("revision"))) {
						throw conflict();
					}
					return;
				}
				requireRevision(current, revision);
				BrainTopicReviewOperations.change(conn, ownerId, ownerType, current, operationId, request);
				int updated = CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_REVIEW SET DRAFT_JSON = ?, REVISION = ?, "
						+ "UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ? AND REVISION = ?",
						CollaborationDbUtils.toJson(current.get("draft")), revision + 1, CollaborationDbUtils.now(),
						ownerId, ownerType, reviewId, revision);
				if (updated != 1) {
					throw conflict();
				}
			});
			return envelope(user, read(ownerId, ownerType, false));
		}
	}

	/** Initialize once, or resume the same draft. Generation is outside the database transaction. */
	public static Map<String, Object> start(User user) {
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock("topic-onboarding", ownerId, ownerType)) {
			Map<String, Object> existing = read(ownerId, ownerType, false);
			// an untouched first draft whose suggestions failed (e.g. not signed in yet) is generated again
			if (existing != null && integer(existing.get("revision")) == 1 && existing.get("appliedRevision") == null
					&& !Objects.toString(map(existing.get("draft")).get("modelError"), "").isEmpty()) {
				CollaborationDbUtils.update("DELETE FROM BRAIN_TOPIC_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND REVIEW_ID = ? AND REVISION = 1", ownerId, ownerType, existing.get("id"));
				existing = null;
			}
			Map<String, Object> current = existing;
			if (current != null) {
				CollaborationDbUtils.batch(conn -> {
					Map<String, Object> locked = requireReview(ownerId, ownerType, (String) current.get("id"), true);
					Map<String, Object> draft = map(locked.get("draft"));
					BrainTopicReviewProfiles.initialize(ownerId, ownerType, draft);
					CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_REVIEW SET DRAFT_JSON = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ?",
							CollaborationDbUtils.toJson(draft), ownerId, ownerType, locked.get("id"));
				});
				return envelope(user, read(ownerId, ownerType, false));
			}
			Map<String, Object> suggestions = BrainTopicSuggest.topics(user);
			List<Map<String, Object>> topics = new ArrayList<>();
			for (Map<String, Object> suggestion : maps(suggestions.get("topics"))) {
				String id = text(suggestion.get("id"), "topic id", 50);
				Map<String, Object> saved = BrainTopicUtils.getTopic(ownerId, ownerType, id);
				Map<String, Object> topic = new LinkedHashMap<>(suggestion);
				topic.put("key", id);
				topic.put("keep", Boolean.TRUE.equals(suggestion.get("suggested")));
				topic.put("description", Objects.toString(saved.get("description"), ""));
				// Suggested short names are automatic. An explicit alias in an accepted topic is retained.
				String shortName = Objects.toString(saved.get("short"), "");
				topic.put("short", !BrainTopicUtils.SUGGESTED.equals(saved.get("status"))
						&& !shortName.equals(saved.get("name")) ? shortName : "");
				topic.put("removedPeople", List.of());
				topic.put("accepted", !BrainTopicUtils.SUGGESTED.equals(saved.get("status")));
				topics.add(topic);
			}
			Map<String, Object> draft = new LinkedHashMap<>();
			draft.put("topics", topics);
			draft.put("modelError", Objects.toString(suggestions.get("modelError"), ""));
			BrainTopicReviewProfiles.initialize(ownerId, ownerType, draft);
			Timestamp now = CollaborationDbUtils.now();
			CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_REVIEW (OWNER_ID, OWNER_TYPE, REVIEW_ID, REVISION, "
					+ "DRAFT_JSON, CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType,
					UUID.randomUUID().toString(), 1, CollaborationDbUtils.toJson(draft), now, now);
			return envelope(user, read(ownerId, ownerType, false));
		}
	}

	/** Save editable profile fields only; source evidence and persisted topic IDs remain server-owned. */
	public static Map<String, Object> save(User user, String reviewId, int revision, Map<String, Object> changes) {
		if (revision < 1) {
			throw conflict();
		}
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock("topic-onboarding", ownerId, ownerType)) {
			CollaborationDbUtils.batch(conn -> {
				Map<String, Object> current = requireReview(ownerId, ownerType, reviewId, true);
				Map<String, Object> draft = map(current.get("draft"));
				BrainTopicReviewProfiles.initialize(ownerId, ownerType, draft);
				List<Map<String, Object>> topics = normalize(changes.get("topics"), maps(draft.get("topics")));
				String guidance = changes.containsKey("guidance") ? text(changes.get("guidance"), "Work context", 6000) : Objects.toString(draft.get("guidance"), "");
				String granularity = changes.containsKey("granularity") ? text(changes.get("granularity"), "Topic detail", 20) : Objects.toString(draft.get("granularity"), "broad");
				if (!Set.of("broad", "projects", "detailed").contains(granularity)) throw new IllegalArgumentException("Choose broad, projects or detailed topic grouping");
				// A retry after a committed-but-lost response is a no-op when the complete draft agrees.
				if (topics.equals(maps(draft.get("topics"))) && Objects.equals(guidance, draft.get("guidance"))
						&& Objects.equals(granularity, draft.get("granularity"))) {
					if (revision > integer(current.get("revision"))) {
						throw conflict();
					}
					return;
				}
				requireRevision(current, revision);
				draft.put("topics", topics);
				draft.put("guidance", guidance);
				draft.put("granularity", granularity);
				int updated = CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_REVIEW SET DRAFT_JSON = ?, "
						+ "REVISION = ?, UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ? "
						+ "AND REVISION = ?", CollaborationDbUtils.toJson(draft), revision + 1,
						CollaborationDbUtils.now(), ownerId, ownerType, reviewId, revision);
				if (updated != 1) {
					throw conflict();
				}
			});
			return envelope(user, read(ownerId, ownerType, false));
		}
	}

	/** Atomically keep/edit/create/remove topics, record their IDs, then start or resume exact filing. */
	public static Map<String, Object> apply(User user, String reviewId, int revision, boolean retryFiling) {
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock("topic-onboarding", ownerId, ownerType)) {
			synchronized (CollaborationDbUtils.ownerLock("job", ownerId, ownerType)) {
				CollaborationDbUtils.batch(conn -> {
					Map<String, Object> current = requireReview(ownerId, ownerType, reviewId, true);
					requireRevision(current, revision);
					if (Objects.equals(current.get("appliedRevision"), revision)) {
						return;
					}
					if (CollaborationJobUtils.anyRunning(ownerId, ownerType)) {
						throw new IllegalArgumentException("Import or sorting is still running. Your review draft is saved; try again after it finishes.");
					}
					Map<String, Object> draft = map(current.get("draft"));
					BrainTopicReviewProfiles.initialize(ownerId, ownerType, draft);
					current.put("draft", draft);
					BrainTopicReviewProfiles.validate(ownerId, ownerType, draft);
					BrainTopicReviewStructure.validateApply(conn, ownerId, ownerType, current);
					BrainTopicReviewOperations.validateApply(ownerId, ownerType, current);
					List<Map<String, Object>> topics = maps(draft.get("topics"));
					List<Map<String, Object>> kept = new ArrayList<>();
					List<String> skipped = new ArrayList<>();
					for (Map<String, Object> topic : topics) {
						String topicId = nullableText(topic.get("id"));
						if (!Boolean.TRUE.equals(topic.get("keep"))) {
							if (topic.get("mergedIntoKey") != null) continue;
							if (topicId != null) {
								if (!BrainTopicUtils.SUGGESTED.equals(BrainTopicUtils.requireTopic(ownerId, ownerType, topicId))) {
									throw new IllegalArgumentException("This topic is already saved. Manage its removal from Topics instead of skipping it in onboarding.");
								}
								BrainTopicUtils.deleteTopic(user, topicId);
								topic.put("id", null);
								skipped.add(topicId);
							}
							continue;
						}
						String name = text(topic.get("name"), "Topic name", 255).trim();
						if (name.isEmpty()) {
							throw new IllegalArgumentException("Every kept topic needs a name");
						}
						String alias = text(topic.get("short"), "Short label", 255).trim();
						Map<String, Object> edit = new LinkedHashMap<>();
						if (topicId != null) {
							edit.put("id", topicId);
						}
						edit.put("name", name);
						edit.put("short", alias.isEmpty() ? name : alias);
						edit.put("description", text(topic.get("description"), "Description", 12000).trim());
						edit.put("keywords", BrainTopicReviewProfiles.terms(topic.get("terms")));
						edit.put("status", BrainTopicUtils.ACTIVE);
						if (topicId == null && nullableText(topic.get("kind")) != null) {
							edit.put("kind", topic.get("kind"));
							edit.put("accountId", topic.get("accountId"));
						}
						Map<String, Object> saved = BrainTopicUtils.saveTopic(user, edit);
						String savedId = (String) saved.get("id");
						if (topicId == null) {
							for (Map<String, Object> person : maps(topic.get("people"))) {
								BrainTopicUtils.setTopicPerson(user, savedId, (String) person.get("id"), BrainTopicUtils.SUGGESTED, null);
							}
						}
						for (String personId : strings(topic.get("removedPeople"))) {
							BrainTopicUtils.setTopicPerson(user, savedId, personId, BrainTopicUtils.REMOVED, null);
						}
						for (String personId : strings(topic.get("appliedRemovedPeople"))) {
							if (!strings(topic.get("removedPeople")).contains(personId)) {
								BrainTopicUtils.setTopicPerson(user, savedId, personId, BrainTopicUtils.SUGGESTED, null);
							}
						}
						topic.put("appliedRemovedPeople", strings(topic.get("removedPeople")));
						topic.put("accepted", true);
						topic.put("id", savedId);
						topic.put("name", saved.get("name"));
						topic.put("description", Objects.toString(saved.get("description"), ""));
						Map<String, Object> receipt = new LinkedHashMap<>();
						for (String field : List.of("id", "name", "short", "description")) {
							receipt.put(field, Objects.toString(saved.get(field), ""));
						}
						receipt.put("key", topic.get("key"));
						kept.add(receipt);
					}
					draft.put("topics", topics);
					List<Map<String, Object>> merges = BrainTopicReviewStructure.apply(user, draft);
					// The retained owner's profile and clues win over the merge helper's keyword union.
					for (Map<String, Object> topic : maps(draft.get("topics"))) {
						if (!Boolean.TRUE.equals(topic.get("keep"))) continue;
						String id = BrainTopicReviewProfiles.id(topic);
						Map<String, Object> saved = BrainTopicUtils.saveTopic(user, Map.of("id", id, "keywords", BrainTopicReviewProfiles.terms(topic.get("terms"))));
						for (String personId : strings(topic.get("removedPeople"))) BrainTopicUtils.setTopicPerson(user, id, personId, BrainTopicUtils.REMOVED, null);
						BrainTopicReviewProfiles.accept(ownerId, ownerType, topic);
						Map<String, Object> receipt = kept.stream().filter(row -> Objects.equals(row.get("key"), topic.get("key"))).findFirst().orElseThrow();
						receipt.put("keywords", saved.get("keywords"));
					}
					List<Map<String, Object>> corrections = BrainTopicReviewOperations.apply(user, ownerId, ownerType, draft, kept);
					// maps() returns copies; retain the refreshed profile bases in the durable draft.
					List<Map<String, Object>> finalTopics = maps(draft.get("topics"));
					finalTopics.forEach(topic -> BrainTopicReviewProfiles.accept(ownerId, ownerType, topic));
					draft.put("topics", finalTopics);
					Map<String, Object> result = Map.of("topics", kept, "skipped", skipped, "corrections", corrections, "merges", merges);
					int updated = CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_REVIEW SET DRAFT_JSON = ?, "
							+ "APPLIED_REVISION = ?, RESULT_JSON = ?, FILING_JOB_ID = NULL, UPDATED_AT = ? "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ? AND REVISION = ?",
							CollaborationDbUtils.toJson(draft), revision, CollaborationDbUtils.toJson(result),
							CollaborationDbUtils.now(), ownerId, ownerType, reviewId, revision);
					if (updated != 1) {
						throw conflict();
					}
				});
				Map<String, Object> current = requireReview(ownerId, ownerType, reviewId, false);
				if (!maps(map(current.get("result")).get("topics")).isEmpty()) {
					startFiling(user, ownerId, ownerType, current, retryFiling);
				}
				return envelope(user, read(ownerId, ownerType, false));
			}
		}
	}

	private static void startFiling(User user, String ownerId, String ownerType, Map<String, Object> review,
			boolean retry) {
		String reviewId = (String) review.get("id");
		int revision = integer(review.get("revision"));
		String jobId = nullableText(review.get("filingJobId"));
		if (jobId == null && !retry) {
			// Recover a crash between enqueuing the job and storing its ID in the receipt.
			for (Map<String, Object> candidate : CollaborationDbUtils.query(
					"SELECT JOB_ID, PARAMS_JSON FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = ? "
							+ "ORDER BY STARTED_AT DESC, JOB_ID DESC", rs -> Map.of("id", rs.getString("JOB_ID"), "params",
							CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "PARAMS_JSON"))), ownerId, ownerType,
					BrainThreadClassifier.JOB_KIND)) {
				Map<String, Object> params = map(candidate.get("params"));
				if (reviewId.equals(params.get("reviewId")) && integer(params.get("reviewRevision")) == revision) {
					jobId = (String) candidate.get("id");
					break;
				}
			}
		}
		Map<String, Object> job = jobId == null ? null : CollaborationJobUtils.get(user, jobId);
		if (job != null) {
			Map<String, Object> params = map(job.get("params"));
			if (!"topics".equals(params.get("mode")) || !reviewId.equals(params.get("reviewId"))
					|| integer(params.get("reviewRevision")) != revision) {
				job = null;
			}
		}
		if (job != null && (!retry || CollaborationJobUtils.RUNNING.equals(job.get("status"))
				|| (CollaborationJobUtils.DONE.equals(job.get("status")) && integer(map(job.get("counts")).get("errors")) == 0))) {
			storeJob(ownerId, ownerType, reviewId, revision, jobId);
			return;
		}
		if (CollaborationJobUtils.anyRunning(ownerId, ownerType)) {
			throw new IllegalArgumentException("Your topics are saved. Another import or sort is running; retry filing after it finishes.");
		}
		job = BrainThreadClassifier.startTopics(user, Map.of("reviewId", reviewId, "reviewRevision", revision));
		Map<String, Object> params = map(job.get("params"));
		if (!"topics".equals(params.get("mode")) || !reviewId.equals(params.get("reviewId"))
				|| integer(params.get("reviewRevision")) != revision) {
			throw new IllegalStateException("Your topics are saved, but their filing job could not be started. Retry filing.");
		}
		storeJob(ownerId, ownerType, reviewId, revision, (String) job.get("id"));
	}

	private static void storeJob(String ownerId, String ownerType, String reviewId, int revision, String jobId) {
		CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_REVIEW SET FILING_JOB_ID = ?, UPDATED_AT = ? "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ? AND APPLIED_REVISION = ?", jobId,
				CollaborationDbUtils.now(), ownerId, ownerType, reviewId, revision);
	}

	private static List<Map<String, Object>> normalize(Object value, List<Map<String, Object>> originals) {
		List<Map<String, Object>> incoming = maps(value);
		if (incoming.size() > MAX_TOPICS) {
			throw new IllegalArgumentException("Review up to " + MAX_TOPICS + " topics at a time");
		}
		Map<String, Map<String, Object>> byKey = new LinkedHashMap<>();
		originals.forEach(topic -> byKey.put((String) topic.get("key"), topic));
		Set<String> seen = new HashSet<>();
		List<Map<String, Object>> normalized = new ArrayList<>();
		for (Map<String, Object> edit : incoming) {
			String key = text(edit.get("key"), "Topic key", 100);
			if (key.isBlank() || !seen.add(key)) {
				throw new IllegalArgumentException("Each draft topic needs a unique key");
			}
			Map<String, Object> original = byKey.get(key);
			String requestedId = nullableText(edit.get("id"));
			if (original == null && (!key.startsWith("added-") || requestedId != null)) {
				throw new IllegalArgumentException("A new draft topic cannot reference an existing topic ID");
			}
			if (original != null && requestedId != null && !requestedId.equals(original.get("id"))) {
				throw new IllegalArgumentException("The draft topic identity has changed; reload the saved review");
			}
			if (!(edit.get("keep") instanceof Boolean)) {
				throw new IllegalArgumentException("Each draft topic needs a keep choice");
			}
			if (original != null && original.get("mergedIntoKey") != null && Boolean.TRUE.equals(edit.get("keep"))) {
				throw new IllegalArgumentException("Undo the grouping instead of reselecting a combined topic");
			}
			if (original != null && original.get("mergedIntoKey") == null && Boolean.TRUE.equals(original.get("accepted")) && !Boolean.TRUE.equals(edit.get("keep"))) {
				throw new IllegalArgumentException("This topic is already saved. Manage its removal from Topics instead of skipping it in onboarding.");
			}
			Map<String, Object> topic = original == null ? new LinkedHashMap<>() : new LinkedHashMap<>(original);
			topic.put("key", key);
			if (original == null) {
				topic.put("id", null);
			}
			for (String field : List.of("name", "description", "short")) {
				topic.put(field, text(edit.get(field), field, "description".equals(field) ? 12000 : 255));
			}
			String clues = edit.containsKey("terms") ? text(edit.get("terms"), "Topic clues", 4000) : Objects.toString(topic.get("terms"), "");
			BrainTopicReviewProfiles.terms(clues);
			topic.put("terms", clues);
			topic.put("keep", edit.get("keep"));
			Set<String> allowedPeople = new HashSet<>();
			maps(topic.get("people")).forEach(person -> allowedPeople.add((String) person.get("id")));
			List<String> removed = strings(edit.get("removedPeople"));
			if (!allowedPeople.containsAll(removed)) {
				throw new IllegalArgumentException("Only people shown on this topic can be removed from its draft");
			}
			topic.put("removedPeople", removed);
			normalized.add(topic);
		}
		for (String key : byKey.keySet()) {
			if (!seen.contains(key)) {
				throw new IllegalArgumentException("Keep the topic in the draft and change its keep choice instead of dropping it");
			}
		}
		return normalized;
	}

	private static Map<String, Object> read(String ownerId, String ownerType, boolean lock) {
		return CollaborationDbUtils.queryOne("SELECT " + COLUMNS + " FROM BRAIN_TOPIC_REVIEW "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?" + (lock ? " FOR UPDATE" : ""), rs -> {
			Map<String, Object> review = new LinkedHashMap<>();
			review.put("id", rs.getString("REVIEW_ID"));
			review.put("revision", rs.getInt("REVISION"));
			review.put("draft", CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "DRAFT_JSON")));
			review.put("appliedRevision", CollaborationDbUtils.getInteger(rs, "APPLIED_REVISION"));
			review.put("result", CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "RESULT_JSON")));
			review.put("filingJobId", CollaborationDbUtils.getString(rs, "FILING_JOB_ID"));
			review.put("updatedAt", CollaborationDbUtils.getTimestamp(rs, "UPDATED_AT"));
			return review;
		}, ownerId, ownerType);
	}

	private static Map<String, Object> requireReview(String ownerId, String ownerType, String reviewId, boolean lock) {
		Map<String, Object> review = read(ownerId, ownerType, lock);
		if (review == null || !Objects.equals(reviewId, review.get("id"))) {
			throw new IllegalArgumentException("Topic review not found for this owner");
		}
		return review;
	}

	private static void requireRevision(Map<String, Object> review, int revision) {
		if (revision < 1 || integer(review.get("revision")) != revision) {
			throw conflict();
		}
	}

	private static IllegalArgumentException conflict() {
		return new IllegalArgumentException("This review was changed elsewhere. Reload the saved review before applying changes.");
	}

	private static Map<String, Object> envelope(User user, Map<String, Object> review) {
		if (review != null) {
			var owner = CollaborationDbUtils.ownerOf(user);
			review.put("profileConflicts", BrainTopicReviewProfiles.conflicts(owner.getValue0(), owner.getValue1(), map(review.get("draft"))));
			String jobId = nullableText(review.get("filingJobId"));
			review.put("filingJob", jobId == null ? null : CollaborationJobUtils.get(user, jobId));
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("exists", review != null);
		result.put("review", review);
		return result;
	}

	private static int integer(Object value) {
		return value instanceof Number number ? number.intValue() : -1;
	}

	private static String nullableText(Object value) {
		return value instanceof String text && !text.isBlank() ? text : null;
	}

	private static String text(Object value, String field, int max) {
		if (value == null) {
			return "";
		}
		if (!(value instanceof String text) || text.length() > max) {
			throw new IllegalArgumentException(field + " must be text of at most " + max + " characters");
		}
		return text;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return value instanceof Map<?, ?> ? new LinkedHashMap<>((Map<String, Object>) value) : new LinkedHashMap<>();
	}

	private static List<Map<String, Object>> maps(Object value) {
		if (value == null) {
			return List.of();
		}
		if (!(value instanceof List<?> values)) {
			throw new IllegalArgumentException("Expected a list of topic records");
		}
		List<Map<String, Object>> result = new ArrayList<>();
		for (Object item : values) {
			if (!(item instanceof Map<?, ?>)) {
				throw new IllegalArgumentException("Expected a topic record");
			}
			result.add(map(item));
		}
		return result;
	}

	private static List<String> strings(Object value) {
		if (!(value instanceof List<?> values)) {
			return List.of();
		}
		List<String> result = new ArrayList<>();
		for (Object item : values) {
			String id = text(item, "Person ID", 50);
			if (id.isBlank()) {
				throw new IllegalArgumentException("Person ID is required");
			}
			if (!result.contains(id)) {
				result.add(id);
			}
		}
		return result;
	}
}
