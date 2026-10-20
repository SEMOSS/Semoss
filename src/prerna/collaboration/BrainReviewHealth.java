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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import prerna.auth.User;

/** Stored, owner-scoped Review measures. Message bodies and model estimates never enter the counts. */
public final class BrainReviewHealth {
    static final String ELIGIBLE = " WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND (t.MUTED IS NULL OR t.MUTED = ?) AND (t.AUTOMATED IS NULL OR t.AUTOMATED = ?)";
    private BrainReviewHealth() { }

    static List<Map<String, Object>> threads(User user) {
        var owner = CollaborationDbUtils.ownerOf(user);
        return CollaborationDbUtils.query("SELECT t.THREAD_ID, t.SUBJECT, t.SOURCE, COALESCE(t.LAST_MESSAGE_AT, t.CREATED_AT) AS ACTIVE_AT FROM BRAIN_THREAD t" + ELIGIBLE + " ORDER BY ACTIVE_AT DESC, t.THREAD_ID", rs -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", rs.getString("THREAD_ID")); row.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
            row.put("source", rs.getString("SOURCE")); row.put("at", CollaborationDbUtils.getTimestamp(rs, "ACTIVE_AT")); return row;
        }, owner.getValue0(), owner.getValue1(), false, false);
    }

    static Map<String, List<Map<String, Object>>> links(User user) {
        var owner = CollaborationDbUtils.ownerOf(user);
        Map<String, List<Map<String, Object>>> links = new HashMap<>();
        CollaborationDbUtils.query("SELECT THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, CLASSIFIER_VERSION FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY THREAD_ID, TOPIC_ID", rs -> {
            Map<String, Object> row = new LinkedHashMap<>(); row.put("topicId", rs.getString("TOPIC_ID"));
            row.put("source", rs.getString("SOURCE")); row.put("confidence", CollaborationDbUtils.getInteger(rs, "CONFIDENCE"));
            row.put("version", rs.getString("CLASSIFIER_VERSION"));
            links.computeIfAbsent(rs.getString("THREAD_ID"), key -> new ArrayList<>()).add(row); return null;
        }, owner.getValue0(), owner.getValue1()); return links;
    }

    static boolean filed(List<Map<String, Object>> links) {
        return links.stream().anyMatch(link -> "you".equals(link.get("source")) || "confirmed".equals(link.get("source")));
    }

    /** Recent mail the classifier filed by itself (same rule as the automatic percentage), newest first. */
    static List<Map<String, Object>> autoFiled(User user, int days, Map<String, String> topicNames) {
        Instant since = Instant.now().minus(Duration.ofDays(days));
        Map<String, List<Map<String, Object>>> links = links(user);
        List<Map<String, Object>> out = new ArrayList<>();
        for (var thread : threads(user)) {
            if (!"email".equals(thread.get("source")) || thread.get("at") == null || Instant.parse((String) thread.get("at")).isBefore(since)) continue;
            List<Map<String, Object>> on = links.getOrDefault(thread.get("id"), List.of());
            boolean manual = on.stream().anyMatch(link -> "you".equals(link.get("source")) || BrainTopicOnboarding.VERSION.equals(link.get("version")));
            if (manual) continue;
            for (var link : on) {
                if (!"confirmed".equals(link.get("source"))) continue;
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("threadId", thread.get("id"));
                row.put("subject", java.util.Objects.toString(thread.get("subject"), "Untitled conversation"));
                row.put("topic", topicNames.getOrDefault((String) link.get("topicId"), "Unavailable topic"));
                row.put("confidence", link.get("confidence"));
                row.put("at", thread.get("at"));
                out.add(row);
            }
        }
        return out;
    }

    public static Map<String, Object> health(User user, int days) {
        if (days < 1 || days > 180) throw new IllegalArgumentException("days must be 1-180");
        var owner = CollaborationDbUtils.ownerOf(user);
        Instant since = Instant.now().minus(Duration.ofDays(days));
        Map<String, List<Map<String, Object>>> links = links(user);
        int total = 0, filed = 0, automatic = 0;
        for (var thread : threads(user)) {
            if (!"email".equals(thread.get("source")) || thread.get("at") == null || Instant.parse((String) thread.get("at")).isBefore(since)) continue;
            total++;
            List<Map<String, Object>> on = links.getOrDefault(thread.get("id"), List.of());
            if (filed(on)) filed++;
            boolean manual = on.stream().anyMatch(link -> "you".equals(link.get("source")) || BrainTopicOnboarding.VERSION.equals(link.get("version")));
            if (!manual && on.stream().anyMatch(link -> "confirmed".equals(link.get("source")))) automatic++;
        }
        Map<String, Integer> questions = new LinkedHashMap<>();
        Timestamp sinceUtc = Timestamp.valueOf(java.time.LocalDateTime.ofInstant(since, java.time.ZoneOffset.UTC));
        CollaborationDbUtils.query("SELECT KIND, COUNT(*) AS N FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = ? AND CREATED_AT >= ? GROUP BY KIND", rs -> { questions.put(rs.getString("KIND"), rs.getInt("N")); return null; }, owner.getValue0(), owner.getValue1(), "open", sinceUtc);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("days", days); result.put("eligibleMailThreads", total); result.put("filedThreads", filed);
        result.put("automaticallyFiledThreads", automatic); result.put("unfiledThreads", total - filed);
        result.put("filedPercent", total == 0 ? null : Math.round(1000.0 * filed / total) / 10.0);
        result.put("automaticPercent", total == 0 ? null : Math.round(1000.0 * automatic / total) / 10.0);
        result.put("decisionsAnswered",CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RESOLVED_BY = ? AND STATUS <> ? AND RESOLVED_AT >= ?",owner.getValue0(),owner.getValue1(),"you","open",sinceUtc));
        result.put("questionsByKind", questions); result.put("questionsWaiting", questions.values().stream().mapToInt(Integer::intValue).sum());
        return result;
    }

    /** Backfills older imports as well as covering the classifier's zero-candidate path. */
    public static void ensureUnassigned(User user) {
        var owner=CollaborationDbUtils.ownerOf(user);
        var links=links(user);
        Map<String,Map<String,Object>> existing=new HashMap<>();
        CollaborationDbUtils.query("SELECT REF_ID, STATUS, RESOLVED_BY FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'unassigned'",rs -> {
            Map<String,Object> row=new LinkedHashMap<>();row.put("status",rs.getString("STATUS"));row.put("by",rs.getString("RESOLVED_BY"));existing.put(rs.getString("REF_ID"),row);return null;
        },owner.getValue0(),owner.getValue1());
        var choices=new java.util.HashSet<>(CollaborationDbUtils.query("SELECT REF_ID FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'topic_choice' AND STATUS = 'open'",rs -> rs.getString(1),owner.getValue0(),owner.getValue1()));
        for (var thread:threads(user)) {
            String id=(String)thread.get("id");var question=existing.get(id);
            if (filed(links.getOrDefault(id,List.of()))) {
                if (question != null && "open".equals(question.get("status"))) CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = 'dismissed', RESOLVED_BY = 'brain', RESOLVED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'unassigned' AND REF_ID = ? AND STATUS = 'open'",CollaborationDbUtils.now(),owner.getValue0(),owner.getValue1(),id);
            } else if (!choices.contains(id)) {
                if (question == null) BrainReviewUtils.addReview(owner.getValue0(),owner.getValue1(),"unassigned","thread",id,"unassigned:v1",Map.of("detail","Choose the topic this conversation belongs to."));
                else if ("brain".equals(question.get("by"))) CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = 'open', RESOLVED_AT = NULL, RESOLVED_BY = NULL WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'unassigned' AND REF_ID = ? AND RESOLVED_BY = 'brain'",owner.getValue0(),owner.getValue1(),id);
            }
        }
    }

    static void ensureUnassigned(String ownerId, String ownerType, String threadId) {
        if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND SOURCE IN ('you', 'confirmed')", ownerId, ownerType, threadId)) return;
        if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'topic_choice' AND REF_ID = ? AND STATUS = 'open'",ownerId,ownerType,threadId)) return;
        CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = 'open', RESOLVED_AT = NULL, RESOLVED_BY = NULL WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'unassigned' AND REF_ID = ? AND RESOLVED_BY = 'brain'",ownerId,ownerType,threadId);
        BrainReviewUtils.addReview(ownerId, ownerType, "unassigned", "thread", threadId, "unassigned:v1", Map.of("detail", "Choose the topic this conversation belongs to."));
    }

    static String memoryVersion(BrainMemoryUtils.Memory memory) {
        return BrainTopicReviewProfiles.hash(List.of(memory.id(),memory.text(),memory.state(),memory.confirmed(),java.util.Objects.toString(memory.replacesId(),"")));
    }

    /** Materializes current suggestions; superseded memory questions are closed without becoming owner decisions. */
    static void ensureMemories(User user) {
        var owner=CollaborationDbUtils.ownerOf(user);
        java.util.Set<String> eligible=new java.util.HashSet<>();
        var existing=new java.util.HashSet<>(CollaborationDbUtils.query("SELECT REVIEW_ID FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'memory'",rs -> rs.getString(1),owner.getValue0(),owner.getValue1()));
        Map<String,List<String>> pending=new HashMap<>();
        CollaborationDbUtils.query("SELECT REF_ID, REVIEW_ID FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'memory' AND STATUS = 'open'",rs -> {pending.computeIfAbsent(rs.getString(1),key -> new ArrayList<>()).add(rs.getString(2));return null;},owner.getValue0(),owner.getValue1());
        boolean usesMemory=BrainMemoryUtils.assistantMemoryOn(user);
        for (var memory:BrainMemoryUtils.load(owner.getValue0(),owner.getValue1(),List.of("active","suggested"))) {
            if (!"suggested".equals(memory.state()) && !("assistant".equals(memory.origin()) && !memory.confirmed())) continue;
            eligible.add(memory.id());String version=memoryVersion(memory);
            String current=CollaborationDbUtils.deterministicId(owner.getValue0(),owner.getValue1(),"review","memory","memory",memory.id(),"memory:"+version);
            if (!existing.contains(current)) BrainReviewUtils.addReview(owner.getValue0(),owner.getValue1(),"memory","memory",memory.id(),"memory:"+version,Map.of("text",memory.text(),"detail","suggested".equals(memory.state()) ? "Brain suggests remembering this. Keep it before it is used." : usesMemory ? "Learned in a chat and in use now. Confirm or dismiss it." : "Learned in a chat. Assistant memory is currently switched off.","memoryAction","suggested".equals(memory.state()) ? "accept" : "confirm","memoryVersion",version));
            if (pending.getOrDefault(memory.id(),List.of()).stream().anyMatch(id -> !id.equals(current))) CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = 'dismissed', RESOLVED_BY = 'brain', RESOLVED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'memory' AND REF_ID = ? AND REVIEW_ID <> ? AND STATUS = 'open'",CollaborationDbUtils.now(),owner.getValue0(),owner.getValue1(),memory.id(),current);
        }
        for (String ref:pending.keySet()) if (!eligible.contains(ref)) BrainReviewUtils.dismissForRef(owner.getValue0(),owner.getValue1(),"memory",ref);
    }

    public static Map<String, Object> overview(User user, int limit, int offset) {
        if (limit < 1 || limit > 100 || offset < 0) throw new IllegalArgumentException("Invalid review pagination");
        ensureUnassigned(user); ensureMemories(user);
        var identity=CollaborationDbUtils.ownerOf(user);
        CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = 'dismissed', RESOLVED_BY = 'brain', RESOLVED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = 'open' AND KIND = 'unknown_sender' AND EXISTS (SELECT 1 FROM BRAIN_TOPIC_PERSON p WHERE p.OWNER_ID = BRAIN_REVIEW.OWNER_ID AND p.OWNER_TYPE = BRAIN_REVIEW.OWNER_TYPE AND p.PERSON_ID = BRAIN_REVIEW.REF_ID AND p.STATE = 'member')",CollaborationDbUtils.now(),identity.getValue0(),identity.getValue1());
        var owner = CollaborationDbUtils.ownerOf(user);
        Map<String, Object> result = new LinkedHashMap<>();
        var links=links(user);
        result.put("filedThreads", threads(user).stream().filter(thread -> filed(links.getOrDefault(thread.get("id"),List.of()))).map(thread -> Map.of("id",thread.get("id"),"name",java.util.Objects.toString(thread.get("subject"),"Untitled conversation"))).toList());
        result.put("health", health(user, 7));
        var pending = BrainReviewUtils.listReview(user, "open", limit, offset);
        Map<String,String> subjects = new HashMap<>();
        threads(user).forEach(thread -> subjects.put((String)thread.get("id"),java.util.Objects.toString(thread.get("subject"),"Untitled conversation")));
        Map<String,String> names = new HashMap<>();
        CollaborationDbUtils.query("SELECT TOPIC_ID, NAME FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ?",rs -> names.put(rs.getString(1),rs.getString(2)),owner.getValue0(),owner.getValue1());
        for (var question : BrainOptimize.rows(pending.get("items"))) {
            String ref = (String)question.get("refId");
            if (subjects.containsKey(ref)) {
                Map<String,Object> data = new LinkedHashMap<>((Map<String,Object>)question.get("data"));
                data.put("subject",subjects.get(ref));
                data.put("currentTopics",links.getOrDefault(ref,List.of()).stream().filter(link -> filed(List.of(link))).map(link -> Map.of("id",link.get("topicId"),"name",names.getOrDefault(link.get("topicId"),"Unavailable topic"))).toList());
                question.put("data",data);
            }
        }
        result.put("pending",pending);
        Map<String,Integer> pendingCounts = new LinkedHashMap<>();
        CollaborationDbUtils.query("SELECT KIND, COUNT(*) FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = 'open' GROUP BY KIND",rs -> pendingCounts.put(rs.getString(1),rs.getInt(2)),owner.getValue0(),owner.getValue1());
        result.put("pendingCounts",pendingCounts);
        result.put("autoFiled", autoFiled(user, 7, names));
        result.put("topics", CollaborationDbUtils.query("SELECT TOPIC_ID, NAME FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS IN ('active', 'dormant') ORDER BY LOWER(NAME), TOPIC_ID", rs -> Map.of("id", rs.getString("TOPIC_ID"), "name", rs.getString("NAME")), owner.getValue0(), owner.getValue1()));
        result.put("job", CollaborationJobUtils.latest(user, BrainOptimize.KIND));
        return result;
    }
}
