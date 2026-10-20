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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

/** Owner-started proposal generation. This service only writes review questions and job progress. */
public final class BrainOptimize {
    public static final String KIND = "optimize";
    static final String MERGE_INSTRUCTIONS = """
            Review the owner's saved topics for likely duplicates. Each topic has an id, name, description
            and conversation count. Group topics together only when they describe the same work with the
            same scope and could reasonably be replaced by one topic. Sharing a client, product or broader
            area is not enough: keep distinct projects, responsibilities and milestones separate.
            When uncertain, leave the topic alone. Use each topic id exactly once, including singletons.
            Return the supplied areas schema: each area has name, about and topics. For a group of duplicates,
            about briefly explains why they appear to describe the same work. These are proposals for the owner.
            """;
    private BrainOptimize() { }

    public static Map<String, Object> start(User user) {
        var owner = CollaborationDbUtils.ownerOf(user);
        return CollaborationJobUtils.start(owner.getValue0(), owner.getValue1(), KIND, Map.of(), job -> {
            // Persisted with the job counts; a stage still "running" when the job fails is the one that failed.
            List<Map<String, Object>> stages = new ArrayList<>();
            job.count("stages", stages);
            job.step("Checking unfiled conversations", 5);
            BrainReviewHealth.ensureUnassigned(user); BrainReviewHealth.ensureMemories(user);
            Insight insight = new Insight(); insight.setUser(user);
            List<String> ids = candidates(user);
            boolean noTopics = BrainThreadClassifier.topicOptions(owner.getValue0(), owner.getValue1()).isEmpty();
            var filingStage = stage(stages, "filing", BrainThreadClassifier.platformEngine());
            int[] scored = {0};
            Function<String, BrainClassifier.Scores> scorer;
            if (ids.isEmpty() || noTopics) {
                scorer = id -> null;
            } else {
                var base = BrainThreadClassifier.reviewScorer(user, insight);
                scorer = id -> { var s = base.apply(id); if (s != null) scored[0]++; return s; };
            }
            Map<String, String> subjects = new HashMap<>();
            BrainReviewHealth.threads(user).forEach(t -> subjects.put((String) t.get("id"), Objects.toString(t.get("subject"), "Untitled conversation")));
            Map<String, String> topicNames = new HashMap<>();
            BrainThreadClassifier.topicOptions(owner.getValue0(), owner.getValue1()).forEach(t -> topicNames.put(t.id(), t.name()));
            List<Map<String, Object>> filingItems = items(filingStage);
            Map<String, Integer> filingResults = new LinkedHashMap<>();
            int filings = proposeFilings(user, ids, scorer, (done, total) -> job.step("Checking filing " + done + " of " + total, 5 + (total == 0 ? 0 : 45 * done / total)),
                    (id, topicId, confidence, result) -> {
                        filingResults.merge(result, 1, Integer::sum);
                        if (filingItems.size() < MAX_ITEMS) filingItems.add(item(subjects.getOrDefault(id, id), topicNames.getOrDefault(topicId, topicId), confidence, result));
                    });
            finish(filingStage, ids.isEmpty() ? "No conversations needed checking" : noTopics ? "No topics to file into" : null, scored[0]);
            filingStage.put("summary", summary(filingResults));
            job.count("filings", filings); job.step("Looking for topics", 55);
            var topicStage = stage(stages, "newTopics", configured(Constants.COLLAB_LLM_ENGINE_ID));
            boolean enough = candidates(user).size() >= 2;
            if (enough) waitForMailboxJobs(job, owner.getValue0(), owner.getValue1());
            var preview = enough ? BrainTopicSuggest.topicsForReview(user) : Map.<String,Object>of("topics",List.of());
            int topicCalls = preview.get("modelCalls") instanceof Number n ? n.intValue() : 0;
            if (preview.get("modelError") != null) {
                topicStage.put("status", "failed"); topicStage.put("calls", topicCalls); topicStage.put("note", brief(preview.get("modelError")));
                throw new IllegalArgumentException(String.valueOf(preview.get("modelError")));
            }
            finish(topicStage, enough ? null : "Fewer than 2 unfiled conversations", topicCalls);
            List<Map<String, Object>> topicItems = items(topicStage);
            int newTopics = proposeTopics(user, rows(preview.get("topics")), (name, threads, result) -> topicItems.add(item(name, null, null, threads + " conversations: " + result)));
            if (enough) topicStage.put("summary", "Grouped " + Objects.toString(preview.get("poolThreads"), "0") + " unfiled conversations into " + rows(preview.get("topics")).size() + " possible topics");
            job.count("newTopics", newTopics); job.step("Checking overlapping topics", 70);
            List<Map<String, Object>> topics = topics(user);
            List<BrainTopicAreas.Topic> cards = topics.stream().map(t -> new BrainTopicAreas.Topic((String)t.get("name"), (String)t.get("about"), ((Number)t.get("threads")).intValue())).toList();
            String engine = BrainTopicModel.engine(user);
            var mergeStage = stage(stages, "merges", engine);
            int[] calls = {0};
            String[] error = {null};
            List<BrainTopicAreas.Area> areas;
            if (engine == null) {
                areas = BrainTopicAreas.single(cards);
            } else {
                var caller = BrainTopicOnboarding.caller(user, engine);
                // BrainTopicAreas.group swallows model errors, so record them before they are hidden.
                areas = mergeGroups((prompt, instructions, params) -> {
                    calls[0]++;
                    try { return caller.ask(prompt, instructions, params); }
                    catch (RuntimeException e) { error[0] = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(); throw e; }
                }, cards);
            }
            if (error[0] != null) {
                mergeStage.put("status", "failed"); mergeStage.put("calls", calls[0]); mergeStage.put("note", brief(error[0]));
            } else {
                finish(mergeStage, engine == null ? "No text model is set" : cards.size() < 2 ? "Fewer than 2 topics" : null, calls[0]);
            }
            job.count("merges", proposeMerges(user, topics, areas));
            List<Map<String, Object>> mergeItems = items(mergeStage);
            for (var area : areas) if (area.members().size() > 1) {
                mergeItems.add(item(area.members().stream().map(i -> (String) topics.get(i).get("name")).collect(java.util.stream.Collectors.joining(" + ")), null, null, "Suggested combining"));
            }
            job.step("Checking topics and people", 85);
            var peopleStage = stage(stages, "topicsAndPeople", null);
            int quiet = proposeQuiet(user), people = proposePeople(user);
            job.count("quietTopics", quiet);
            job.count("people", people);
            finish(peopleStage, null, 0);
            peopleStage.put("summary", quiet + " new quiet-topic and " + people + " new people suggestions");
            job.step("Review is ready", 99);
        });
    }

    static Map<String, Object> stage(List<Map<String, Object>> stages, String key, String engineId) {
        Map<String, Object> stage = new LinkedHashMap<>();
        stage.put("key", key);
        if (engineId != null) {
            String alias = SecurityEngineUtils.getEngineAliasForId(engineId);
            stage.put("model", alias == null || alias.isBlank() ? engineId : alias);
        }
        stage.put("status", "running");
        stages.add(stage);
        return stage;
    }

    // a skip note marks the stage skipped; otherwise it completed with this many model answers
    static void finish(Map<String, Object> stage, String skipped, int calls) {
        stage.put("status", skipped == null ? "ok" : "skipped");
        stage.put("calls", calls);
        if (skipped != null) stage.put("note", skipped);
    }

    static String configured(String key) {
        String id = Utility.getDIHelperProperty(key);
        return id == null || id.isBlank() ? null : id.trim();
    }

    static String brief(Object message) {
        String text = String.valueOf(message);
        return text.length() > 300 ? text.substring(0, 300) + "…" : text;
    }

    static final int MAX_ITEMS = 200;

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> items(Map<String, Object> stage) {
        return (List<Map<String, Object>>) stage.computeIfAbsent("items", k -> new ArrayList<Map<String, Object>>());
    }

    static Map<String, Object> item(String label, String topic, Integer confidence, String result) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("label", label);
        if (topic != null) item.put("topic", topic);
        if (confidence != null) item.put("confidence", confidence);
        item.put("result", result);
        return item;
    }

    static String summary(Map<String, Integer> results) {
        return results.entrySet().stream().map(e -> e.getValue() + " " + e.getKey().toLowerCase()).collect(java.util.stream.Collectors.joining(", "));
    }

    // the page's periodic mail sync also holds the mailbox; topic grouping refuses to read it mid-import
    static void waitForMailboxJobs(CollaborationJobUtils.Job job, String ownerId, String ownerType) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
        while (CollaborationJobUtils.anyRunning(ownerId, ownerType, KIND) && System.currentTimeMillis() < deadline) {
            job.step("Waiting for mail sync to finish", 55);
            Thread.sleep(2000);
        }
    }

    interface FilingOutcome { void report(String threadId, String topicId, Integer confidence, String result); }

    interface TopicOutcome { void report(String name, int threads, String result); }

    interface Progress { void report(int done, int total); }

    static List<BrainTopicAreas.Area> mergeGroups(BrainTopicVotes.Caller caller, List<BrainTopicAreas.Topic> topics) {
        // Reuse the existing schema and validation, with a stricter duplicate-topic judgment for Review.
        return BrainTopicAreas.group((prompt,instructions,params) -> caller.ask(prompt,MERGE_INSTRUCTIONS,params),topics);
    }

    static List<String> candidates(User user) {
        var links = BrainReviewHealth.links(user);
        var owner = CollaborationDbUtils.ownerOf(user);
        int fileAt = ((Number) BrainProfileUtils.getSettings(owner.getValue0(), owner.getValue1()).get("fileAt")).intValue();
        return BrainReviewHealth.threads(user).stream().map(t -> (String)t.get("id")).filter(id -> {
            var on = links.getOrDefault(id, List.of());
            return on.stream().noneMatch(l -> "you".equals(l.get("source")) || BrainTopicOnboarding.VERSION.equals(l.get("version")))
                    && (!BrainReviewHealth.filed(on) || on.stream().anyMatch(l -> l.get("confidence") instanceof Number n && n.intValue() < fileAt));
        }).toList();
    }

    static int proposeFilings(User user, List<String> ids, Function<String, BrainClassifier.Scores> scorer, Progress progress) {
        return proposeFilings(user, ids, scorer, progress, (id, topic, confidence, result) -> {});
    }

    static int proposeFilings(User user, List<String> ids, Function<String, BrainClassifier.Scores> scorer, Progress progress, FilingOutcome outcome) {
        var owner = CollaborationDbUtils.ownerOf(user); int found = 0, done = 0;
        Set<String> valid = new HashSet<>(BrainThreadClassifier.topicOptions(owner.getValue0(), owner.getValue1()).stream().map(BrainClassifier.TopicOption::id).toList());
        var settings = BrainProfileUtils.getSettings(owner.getValue0(), owner.getValue1());
        int askAt = ((Number)settings.get("askAt")).intValue();
        int fileAt = ((Number)settings.get("fileAt")).intValue();
        for (String id : ids) {
            BrainClassifier.Scores scores = scorer.apply(id);
            if (scores != null) {
                var rejected = BrainThreadTopicDecisions.rejected(owner.getValue0(), owner.getValue1(), id);
                var ranked=scores.topics().entrySet().stream().filter(e -> valid.contains(e.getKey()) && !rejected.contains(e.getKey())).sorted((a,b) -> { int c=Double.compare(b.getValue(),a.getValue()); return c != 0 ? c : a.getKey().compareTo(b.getKey()); }).toList();
                boolean wayOut=scores.topics().containsKey(BrainThreadClassifier.OTHER_TOPIC);
                int confidence=ranked.isEmpty() ? 0 : (int)Math.min(100,Math.round(wayOut ? 100*ranked.get(0).getValue() : 900*(ranked.get(0).getValue()-(ranked.size()>1 ? ranked.get(1).getValue() : 0))));
                int threshold=wayOut ? Math.max(askAt,fileAt-BrainThreadClassifier.WAY_OUT_ASK_BAND) : askAt;
                List<String> choices=ranked.isEmpty() || BrainThreadClassifier.OTHER_TOPIC.equals(ranked.get(0).getKey()) || confidence<threshold ? List.of() : ranked.stream().filter(e -> !BrainThreadClassifier.OTHER_TOPIC.equals(e.getKey())).limit(confidence>=fileAt ? 1 : 2).map(Map.Entry::getKey).toList();
                String best = ranked.isEmpty() ? null : ranked.get(0).getKey();
                if (choices.isEmpty()) {
                    outcome.report(id, best, confidence, BrainThreadClassifier.OTHER_TOPIC.equals(best) ? "Best match is Something else" : "Not confident enough (needs " + threshold + "%)");
                }
                if (!choices.isEmpty()) {
                    // Do not create a second question alongside a pending classifier topic choice.
                    boolean pending = CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REF_TYPE = 'thread' AND REF_ID = ? AND KIND = 'topic_choice' AND STATUS = 'open'", owner.getValue0(), owner.getValue1(), id);
                    if (pending) outcome.report(id, best, confidence, "Already waiting for your answer");
                    if (!pending) {
                        String key = "optimize-filing:" + String.join(":", choices.stream().sorted().toList());
                        var proposal = BrainReviewUtils.addReview(owner.getValue0(), owner.getValue1(), "topic_choice", "thread", id, key, Map.of("candidates", choices, "detail", "Review this filing against your current topics. Nothing has moved."));
                        if (Boolean.TRUE.equals(proposal.get("created"))) found++;
                        outcome.report(id, best, confidence, Boolean.TRUE.equals(proposal.get("created")) ? "Suggested" : "Suggested earlier");
                        CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = 'dismissed', RESOLVED_BY = 'brain', RESOLVED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'unassigned' AND REF_ID = ? AND STATUS = 'open'",CollaborationDbUtils.now(),owner.getValue0(),owner.getValue1(),id);
                    }
                }
            }
            progress.report(++done, ids.size());
        }
        return found;
    }

    static int proposeTopics(User user, List<Map<String, Object>> candidates) {
        return proposeTopics(user, candidates, (name, threads, result) -> {});
    }

    static int proposeTopics(User user, List<Map<String, Object>> candidates, TopicOutcome outcome) {
        var owner = CollaborationDbUtils.ownerOf(user); int found = 0;
        var links = BrainReviewHealth.links(user);
        for (var candidate : candidates) {
            List<String> threads = strings(candidate.get("threadIds")).stream().filter(id -> !BrainReviewHealth.filed(links.getOrDefault(id, List.of()))).sorted().toList();
            String name = Objects.toString(candidate.get("name"), "Unnamed group");
            if (threads.size() < 2) { outcome.report(name, threads.size(), "not suggested, fewer than 2 still unfiled"); continue; }
            Map<String, Object> detail = new LinkedHashMap<>(candidate);
            detail.put("text", "New topic: " + candidate.get("name")); detail.put("detail", "Based on " + threads.size() + " unfiled conversations. Created only if you accept."); detail.put("threadIds", threads);
            var proposal = BrainReviewUtils.addReview(owner.getValue0(), owner.getValue1(), "new_topic", "thread", threads.get(0), "optimize-topic:" + String.join(":", threads), detail);
            if (Boolean.TRUE.equals(proposal.get("created"))) found++;
            outcome.report(name, threads.size(), Boolean.TRUE.equals(proposal.get("created")) ? "suggested" : "suggested earlier");
        }
        return found;
    }

    static List<Map<String, Object>> topics(User user) {
        var owner = CollaborationDbUtils.ownerOf(user);
        return CollaborationDbUtils.query("SELECT t.TOPIC_ID, t.NAME, t.DESCRIPTION, t.CREATED_AT, (SELECT COUNT(*) FROM BRAIN_THREAD_TOPIC l WHERE l.OWNER_ID = t.OWNER_ID AND l.OWNER_TYPE = t.OWNER_TYPE AND l.TOPIC_ID = t.TOPIC_ID AND l.SOURCE IN ('you','confirmed')) AS N FROM BRAIN_TOPIC t WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND t.STATUS IN ('active','dormant') ORDER BY t.TOPIC_ID", rs -> {
            Map<String,Object> row = new LinkedHashMap<>(); row.put("id", rs.getString("TOPIC_ID")); row.put("name", rs.getString("NAME")); row.put("about", CollaborationDbUtils.getString(rs,"DESCRIPTION")); row.put("threads", rs.getInt("N")); row.put("createdAt",CollaborationDbUtils.getTimestamp(rs,"CREATED_AT")); return row;
        }, owner.getValue0(), owner.getValue1());
    }

    static int proposeMerges(User user, List<Map<String, Object>> topics, List<BrainTopicAreas.Area> areas) {
        var owner = CollaborationDbUtils.ownerOf(user); int found = 0;
        for (var area : areas) for (int i = 1; i < area.members().size(); i++) {
            var first = topics.get(area.members().get(0)); var other = topics.get(area.members().get(i));
            var proposal = BrainReviewUtils.addReview(owner.getValue0(), owner.getValue1(), "merge_topic", "topic", (String)other.get("id"), "optimize-merge:" + first.get("id"), Map.of("text", "Combine " + other.get("name") + " with " + first.get("name") + "?", "detail", Objects.toString(area.about(),"These topics may describe the same ongoing work: " + area.name()), "targetTopicId", first.get("id")));
            if (Boolean.TRUE.equals(proposal.get("created"))) found++;
        }
        return found;
    }

    static int proposeQuiet(User user) {
        var owner = CollaborationDbUtils.ownerOf(user); int found = 0;
        Instant cutoff = Instant.now().minus(Duration.ofDays(30));
        Map<String, String> activity = new HashMap<>();
        var links = BrainReviewHealth.links(user);
        for (var thread : BrainReviewHealth.threads(user)) for (var link : links.getOrDefault(thread.get("id"), List.of())) {
            if (thread.get("at") != null) activity.merge((String)link.get("topicId"), (String)thread.get("at"), (a,b) -> a.compareTo(b) >= 0 ? a : b);
        }
        for (var topic : topics(user)) {
            String at = activity.getOrDefault(topic.get("id"),(String)topic.get("createdAt")); if (at != null && !Instant.parse(at).isBefore(cutoff)) continue;
            var proposal = BrainReviewUtils.addReview(owner.getValue0(), owner.getValue1(), "quiet_topic", "topic", (String)topic.get("id"), "quiet:v1", Map.of("text", "Archive quiet topic " + topic.get("name") + "?", "detail", "No conversation activity in the last 30 days. Archiving keeps its history."));
            if (Boolean.TRUE.equals(proposal.get("created"))) found++;
        }
        return found;
    }

    static int proposePeople(User user) {
        var owner = CollaborationDbUtils.ownerOf(user); int found = 0;
        var people = CollaborationDbUtils.query("SELECT p.PERSON_ID, p.DISPLAY_NAME, p.EMAIL_NORM FROM BRAIN_PERSON p WHERE p.OWNER_ID = ? AND p.OWNER_TYPE = ? AND (p.RELATIONSHIP IS NULL OR p.RELATIONSHIP <> 'self') AND (p.NEVER_INGEST IS NULL OR p.NEVER_INGEST = ?) AND (p.RELATIONSHIP IS NULL OR p.RELATIONSHIP <> 'automated')", rs -> Map.of("id",rs.getString(1),"name", Objects.toString(rs.getString(2),"Unknown sender"),"email",Objects.toString(rs.getString(3),"")), owner.getValue0(),owner.getValue1(),false);
        var links = BrainReviewHealth.links(user);
        Instant since = Instant.now().minus(Duration.ofDays(7));
        var recentThreads = BrainReviewHealth.threads(user).stream().filter(t -> t.get("at") != null && !Instant.parse((String)t.get("at")).isBefore(since)).toList();
        Set<String> recent = new HashSet<>(recentThreads.stream().map(t -> (String)t.get("id")).toList());
        Map<String,String> sources = new HashMap<>();
        recentThreads.forEach(t -> sources.put((String)t.get("id"),(String)t.get("source")));
        var rules=BrainRulesGate.activeRules(owner.getValue0(),owner.getValue1());
        for (var person : people) {
            String id = person.get("id");
            if (BrainRulesGate.neverRule(rules,person.get("email"),id,null) != null) continue;
            Set<String> written = new HashSet<>(CollaborationDbUtils.query("SELECT DISTINCT THREAD_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SENDER_PERSON_ID = ?", rs -> rs.getString(1), owner.getValue0(),owner.getValue1(),id));
            written.retainAll(recent);
            written.removeIf(thread -> CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ? AND INCLUDED = ?",owner.getValue0(),owner.getValue1(),thread,id,false)
                || BrainRulesGate.exclusionRule(rules,links.getOrDefault(thread,List.of()).stream().map(link -> (String)link.get("topicId")).toList(),sources.get(thread),id) != null);
            Map<String,Integer> byTopic = new LinkedHashMap<>();
            for (String thread : written) for (var link : links.getOrDefault(thread,List.of())) if (BrainReviewHealth.filed(List.of(link))) byTopic.merge((String)link.get("topicId"),1,Integer::sum);
            for (var entry : byTopic.entrySet()) {
                if (entry.getValue() < 3 || CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND PERSON_ID = ? AND STATE IN ('member','removed')", owner.getValue0(),owner.getValue1(),entry.getKey(),id)) continue;
                var proposal = BrainReviewUtils.addReview(owner.getValue0(), owner.getValue1(), "add_person", "person", id, "person-topic:"+entry.getKey(), Map.of("text","Add " + person.get("name") + " to this topic?", "detail", "Sent messages in " + entry.getValue() + " recent conversations on the topic.", "suggestedTopic",entry.getKey()));
                if (Boolean.TRUE.equals(proposal.get("created"))) found++;
            }
            if (written.size() >= 3 && !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ? AND STATE IN ('member','removed')", owner.getValue0(),owner.getValue1(),id)) {
                var proposal = BrainReviewUtils.addReview(owner.getValue0(),owner.getValue1(),"unknown_sender","person",id,"unknown-sender:v1",Map.of("text","Where does " + person.get("name") + " belong?", "detail","A sender in " + written.size() + " recent conversations with no confirmed topic membership."));
                if (Boolean.TRUE.equals(proposal.get("created"))) found++;
            }
        }
        return found;
    }

    @SuppressWarnings("unchecked") static List<Map<String,Object>> rows(Object value) { return value instanceof List<?> list ? list.stream().filter(Map.class::isInstance).map(row -> (Map<String,Object>)row).toList() : List.of(); }
    static List<String> strings(Object value) { return value instanceof List<?> list ? list.stream().filter(String.class::isInstance).map(String.class::cast).toList() : List.of(); }
}
