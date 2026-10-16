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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import prerna.auth.User;

/** Applies one owner answer and records its reversible effect in the existing review row, atomically. */
public final class BrainReviewDecisions {
    private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";
    private static final Set<String> TABLES = Set.of("BRAIN_THREAD_TOPIC", "BRAIN_THREAD_TOPIC_REJECTION", "BRAIN_TOPIC", "BRAIN_TOPIC_PERSON", "BRAIN_MEMORY", "WORK_ITEM");
    private BrainReviewDecisions() { }

    record Scope(String table, String column, String value, String otherColumn, String otherValue) {
        String where() { return OWNED + " AND " + column + " = ?" + (otherColumn == null ? "" : " AND " + otherColumn + " = ?"); }
        Object[] params(String owner, String type) { return otherColumn == null ? new Object[]{owner,type,value} : new Object[]{owner,type,value,otherValue}; }
    }

    public static Map<String,Object> answer(User user, String reviewId, String action, String topicId) {
        if (!Set.of("accept","dismiss","both","merge").contains(action)) throw new IllegalArgumentException("Unsupported Review answer");
        var owner = CollaborationDbUtils.ownerOf(user);
        return BrainTopicReviewProfiles.serialized(owner.getValue0(),owner.getValue1(), () -> {
            synchronized (CollaborationDbUtils.ownerLock("review",owner.getValue0(),owner.getValue1())) {
                List<Map<String,Object>> result = new ArrayList<>();
                CollaborationDbUtils.batch(conn -> result.add(apply(conn,user,reviewId,action,topicId)));
                return result.get(0);
            }
        });
    }

    /** An explicit owner request can correct an owner-made filing; optimization itself never changes it. */
    public static Map<String,Object> requestCorrection(User user,String threadId) {
        var owner=CollaborationDbUtils.ownerOf(user);
        BrainThreadUtils.requireThread(owner.getValue0(),owner.getValue1(),threadId);
        var links=BrainReviewHealth.links(user).getOrDefault(threadId,List.of());
        if (!BrainReviewHealth.filed(links)) throw new IllegalArgumentException("This conversation has no confirmed filing to correct");
        String version=BrainTopicReviewProfiles.hash(links);
        var result=BrainReviewUtils.addReview(owner.getValue0(),owner.getValue1(),"wrong_topic","thread",threadId,"correction:"+version,Map.of("text","Correct filing: "+threadName(owner.getValue0(),owner.getValue1(),threadId),"detail","Choose the correct topic. The current filing changes only when you save your answer.","expectedLinksHash",version));
        var review=map(result.get("review"));
        if (!"open".equals(review.get("status"))) throw new IllegalArgumentException("You already reviewed this filing. Undo that decision in Learned recently to revisit it.");
        return review;
    }

    private static Map<String,Object> apply(Connection conn, User user, String reviewId, String action, String topicId) throws SQLException {
        var owner = CollaborationDbUtils.ownerOf(user); String oid=owner.getValue0(), type=owner.getValue1();
        var review = BrainReviewUtils.getReview(user,reviewId);
        if (!"open".equals(review.get("status"))) throw new IllegalArgumentException("This question was already answered. Refresh Review.");
        String kind=(String)review.get("kind"), ref=(String)review.get("refId");
        Map<String,Object> data=map(review.get("data"));
        if ("memory".equals(kind)) {
            var current=BrainMemoryUtils.find(oid,type,ref);
            if (current == null || !java.util.Objects.equals(data.get("memoryVersion"),BrainReviewHealth.memoryVersion(current))) throw new IllegalArgumentException("This memory changed since the question was opened. Refresh Review.");
        }
        List<Scope> scopes = "dismiss".equals(action) && Set.of("add_person","unknown_sender").contains(kind) ? new ArrayList<>() : scopes(kind,ref,topicId,data);
        if ("memory".equals(kind)) {
            var memory = BrainMemoryUtils.find(oid,type,ref);
            if (memory != null && memory.replacesId() != null) scopes.add(new Scope("BRAIN_MEMORY","MEMORY_ID",memory.replacesId(),null,null));
        }
        Map<String,Object> before = snapshot(conn,oid,type,scopes);
        Map<String,Object> resolution = new LinkedHashMap<>();
        resolution.put("action",action); resolution.put("summary", ("dismiss".equals(action) ? "Dismissed: " : "Answered: ") + review.get("text"));
        resolution.put("applied",true);
        // Resolve first: a topic merge can dismiss still-open questions about its source.
        BrainReviewUtils.resolveReview(user,reviewId,action,topicId == null ? Map.of() : Map.of("topicId",topicId));
        if (!"dismiss".equals(action)) {
            switch (kind) {
                case "unassigned", "topic_choice", "wrong_topic" -> {
                    if ("wrong_topic".equals(kind)) {
                        if (!java.util.Objects.equals(data.get("expectedLinksHash"),BrainTopicReviewProfiles.hash(BrainReviewHealth.links(user).getOrDefault(ref,List.of())))) throw new IllegalArgumentException("The filing changed since this question was opened. Refresh Review.");
                    } else requireUnchangedFiling(oid,type,ref);
                    List<String> choices = "both".equals(action) ? BrainOptimize.strings(data.get("candidates")) : List.of(required(topicId,"Pick a topic"));
                    if (choices.isEmpty()) throw new IllegalArgumentException("This question has no candidate topics");
                    if ("topic_choice".equals(kind) && !BrainOptimize.strings(data.get("candidates")).containsAll(choices)) throw new IllegalArgumentException("Choose a candidate from this question");
                    for (String id : choices) requireActiveTopic(user,id);
                    for (var link : BrainReviewHealth.links(user).getOrDefault(ref,List.of())) if (!choices.contains(link.get("topicId"))) BrainThreadUtils.linkThreadTopic(user,ref,(String)link.get("topicId"),false,true);
                    for (int i=0;i<choices.size();i++) BrainThreadUtils.linkThreadTopic(user,ref,choices.get(i),i==0,false);
                    String primary=choices.get(0);
                    CollaborationDbUtils.update("UPDATE WORK_ITEM SET LINK_TOPIC_ID = ?"+OWNED+" AND THREAD_ID = ?",primary,oid,type,ref);
                    resolution.put("summary","Filed " + threadName(oid,type,ref) + " under " + String.join(", ", choices.stream().map(id -> (String)BrainTopicUtils.getTopic(user,id).get("name")).toList()));
                }
                case "new_topic" -> {
                    String name=required(CollaborationDbUtils.asString(data.get("name")),"The topic suggestion has no name");
                    Map<String,Object> changes=new LinkedHashMap<>(); changes.put("name",name); changes.put("kind", data.getOrDefault("kind","internal")); changes.put("status","active");
                    if (data.get("about") != null) changes.put("description", data.get("about"));
                    var created=BrainTopicUtils.saveTopic(user,changes); String id=(String)created.get("id");
                    // The new UUID did not exist in the before image.
                    for (Scope scope : List.of(new Scope("BRAIN_TOPIC","TOPIC_ID",id,null,null),new Scope("BRAIN_TOPIC_PERSON","TOPIC_ID",id,null,null),new Scope("BRAIN_THREAD_TOPIC","TOPIC_ID",id,null,null))) { scopes.add(scope); before.put(Integer.toString(scopes.size()-1),List.of()); }
                    for (String thread : BrainOptimize.strings(data.get("threadIds"))) {
                        if (BrainReviewHealth.filed(BrainReviewHealth.links(user).getOrDefault(thread,List.of()))) throw new IllegalArgumentException("A suggested conversation has been filed since. Refresh Review.");
                        BrainThreadUtils.linkThreadTopic(user,thread,id,true,false);
                        CollaborationDbUtils.update("UPDATE WORK_ITEM SET LINK_TOPIC_ID = ?"+OWNED+" AND THREAD_ID = ?",id,oid,type,thread);
                    }
                    for (String person : BrainOptimize.strings(data.get("memberIds"))) BrainTopicUtils.setTopicPerson(user,id,person,"member",null);
                    resolution.put("summary","Created topic " + name); resolution.put("topicId",id);
                }
                case "merge_topic" -> {
                    String target=required(CollaborationDbUtils.asString(data.get("targetTopicId")),"Missing merge destination");
                    requireActiveTopic(user,ref); requireActiveTopic(user,target);
                    String sourceName=(String)BrainTopicUtils.getTopic(user,ref).get("name");String targetName=(String)BrainTopicUtils.getTopic(user,target).get("name");
                    var merged=BrainTopicUtils.mergeTopics(user,ref,target);
                    resolution.put("changeId",merged.get("changeId")); resolution.put("summary","Merged " + sourceName + " into " + targetName);
                }
                case "quiet_topic" -> {
                    requireActiveTopic(user,ref);
                    Timestamp cutoff=Timestamp.valueOf(java.time.LocalDateTime.ofInstant(Instant.now().minus(Duration.ofDays(30)),java.time.ZoneOffset.UTC));
                    if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD t JOIN BRAIN_THREAD_TOPIC l ON l.OWNER_ID = t.OWNER_ID AND l.OWNER_TYPE = t.OWNER_TYPE AND l.THREAD_ID = t.THREAD_ID"+BrainReviewHealth.ELIGIBLE+" AND l.TOPIC_ID = ? AND COALESCE(t.LAST_MESSAGE_AT,t.CREATED_AT) >= ?",oid,type,false,false,ref,cutoff)) throw new IllegalArgumentException("This topic has recent activity. Refresh Review before archiving it.");
                    String name=(String)BrainTopicUtils.getTopic(user,ref).get("name"); BrainTopicUtils.setTopicStatus(user,ref,"archived"); resolution.put("summary","Archived topic " + name);
                }
                case "add_person", "unknown_sender" -> {
                    String id=topicId == null ? CollaborationDbUtils.asString(data.get("suggestedTopic")) : topicId;
                    requireActiveTopic(user,required(id,"Pick a topic"));
                    BrainTopicUtils.setTopicPerson(user,id,ref,"member",null); resolution.put("summary", "Added " + BrainPeopleUtils.getPerson(oid,type,ref).get("name") + " to " + BrainTopicUtils.getTopic(user,id).get("name"));
                }
                case "memory" -> {
                    BrainMemoryUtils.resolveMemory(user,ref, required(CollaborationDbUtils.asString(data.get("memoryAction")),"Missing memory action"));
                    resolution.put("summary","Kept memory: " + review.get("text"));
                }
                default -> throw new IllegalArgumentException("Unsupported review kind");
            }
        } else if ("memory".equals(kind)) {
            BrainMemoryUtils.resolveMemory(user,ref,"dismiss");
        } else if ("topic_choice".equals(kind)) {
            for (String id : BrainOptimize.strings(data.get("candidates"))) if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC"+OWNED+" AND THREAD_ID = ? AND TOPIC_ID = ? AND SOURCE IN ('you','confirmed')",oid,type,ref,id)) BrainThreadTopicDecisions.remember(oid,type,ref,id,true);
        }
        resolution.put("scopes",scopes); resolution.put("before",before); resolution.put("after",snapshot(conn,oid,type,scopes));
        CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET RESOLUTION_JSON = ?"+OWNED+" AND REVIEW_ID = ?",CollaborationDbUtils.toJson(resolution),oid,type,reviewId);
        var applied=BrainReviewUtils.getReview(user,reviewId);
        if ("merge_topic".equals(kind) && !"dismiss".equals(action)) applied.put("removedTopicIds",List.of(ref));
        return applied;
    }

    private static void requireUnchangedFiling(String owner,String type,String thread) {
        BrainThreadUtils.requireThread(owner,type,thread);
        if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC"+OWNED+" AND THREAD_ID = ? AND (SOURCE = 'you' OR CLASSIFIER_VERSION = ?)",owner,type,thread,BrainTopicOnboarding.VERSION)) throw new IllegalArgumentException("The owner has already filed this conversation. Refresh Review.");
    }
    private static void requireActiveTopic(User user,String id) {
        Object status = BrainTopicUtils.getTopic(user,id).get("status");
        if (!"active".equals(status) && !"dormant".equals(status)) throw new IllegalArgumentException("Choose an active or dormant topic");
    }
    private static String required(String value,String message) { if (value == null || value.isBlank()) throw new IllegalArgumentException(message); return value; }
    private static String threadName(String owner,String type,String id) { return CollaborationDbUtils.queryOne("SELECT SUBJECT FROM BRAIN_THREAD"+OWNED+" AND THREAD_ID = ?",rs -> java.util.Objects.toString(rs.getString(1),"conversation"),owner,type,id); }

    private static List<Scope> scopes(String kind,String ref,String topic,Map<String,Object> data) {
        List<Scope> scopes=new ArrayList<>();
        List<String> threads=Set.of("unassigned","topic_choice","wrong_topic").contains(kind) ? List.of(ref) : "new_topic".equals(kind) ? BrainOptimize.strings(data.get("threadIds")) : List.of();
        for (String id : threads) for (String table : List.of("BRAIN_THREAD_TOPIC","BRAIN_THREAD_TOPIC_REJECTION","WORK_ITEM")) scopes.add(new Scope(table,"THREAD_ID",id,null,null));
        if (Set.of("add_person","unknown_sender").contains(kind)) scopes.add(new Scope("BRAIN_TOPIC_PERSON","PERSON_ID",ref,"TOPIC_ID",required(topic == null ? CollaborationDbUtils.asString(data.get("suggestedTopic")) : topic,"Pick a topic")));
        if ("quiet_topic".equals(kind)) scopes.add(new Scope("BRAIN_TOPIC","TOPIC_ID",ref,null,null));
        if ("memory".equals(kind)) {
            scopes.add(new Scope("BRAIN_MEMORY","MEMORY_ID",ref,null,null));
            // Accept/Dismiss can also supersede or restore the memory it replaces.
        }
        return scopes;
    }

    static Map<String,Object> snapshot(Connection conn,String owner,String type,List<Scope> scopes) throws SQLException {
        Map<String,Object> snapshot=new LinkedHashMap<>();
        for (int i=0;i<scopes.size();i++) {
            Scope scope=scopes.get(i);
            if (!TABLES.contains(scope.table())) throw new IllegalArgumentException("Invalid decision snapshot");
            String columns="WORK_ITEM".equals(scope.table()) ? "ITEM_ID, LINK_TOPIC_ID" : "*";
            var rows=BrainTopicChangeUtils.readRows(conn,"SELECT "+columns+" FROM "+scope.table()+scope.where(),scope.params(owner,type));
            rows = BrainOptimize.rows(CollaborationDbUtils.parseList(CollaborationDbUtils.toJson(rows)));
            rows = new ArrayList<>(rows);
            rows.sort(java.util.Comparator.comparing(BrainTopicReviewProfiles::hash));
            snapshot.put(Integer.toString(i),rows);
        }
        return snapshot;
    }

    public static Map<String,Object> undo(User user,String reviewId) {
        var owner=CollaborationDbUtils.ownerOf(user); String oid=owner.getValue0(), type=owner.getValue1();
        return BrainTopicReviewProfiles.serialized(oid,type,() -> {
            synchronized(CollaborationDbUtils.ownerLock("review",oid,type)) {
                List<Map<String,Object>> result=new ArrayList<>();
                CollaborationDbUtils.batch(conn -> {
                    var review=BrainReviewUtils.getReview(user,reviewId);
                    if ("open".equals(review.get("status")) || !"you".equals(review.get("resolvedBy"))) throw new IllegalArgumentException("This owner decision cannot be undone");
                    Map<String,Object> resolution=CollaborationDbUtils.queryOne("SELECT RESOLUTION_JSON FROM BRAIN_REVIEW"+OWNED+" AND REVIEW_ID = ?",rs -> CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs,"RESOLUTION_JSON")),oid,type,reviewId);
                    if (resolution == null || !Boolean.TRUE.equals(resolution.get("applied"))) throw new IllegalArgumentException("This older decision has no reversible change recorded");
                    if (resolution.get("changeId") instanceof String id) BrainTopicChangeUtils.undo(user,id);
                    else {
                        List<Scope> scopes=new ArrayList<>();
                        for (var scope : BrainOptimize.rows(resolution.get("scopes"))) scopes.add(new Scope((String)scope.get("table"),(String)scope.get("column"),(String)scope.get("value"),(String)scope.get("otherColumn"),(String)scope.get("otherValue")));
                        if (!BrainTopicReviewProfiles.hash(snapshot(conn,oid,type,scopes)).equals(BrainTopicReviewProfiles.hash(resolution.get("after")))) throw new IllegalArgumentException("This data changed after your answer. Undo was not applied.");
                        Map<String,Object> before=map(resolution.get("before"));
                        for (Scope scope : scopes) if (!"WORK_ITEM".equals(scope.table())) CollaborationDbUtils.update(conn,"DELETE FROM "+scope.table()+scope.where(),scope.params(oid,type));
                        Map<String,Map<String,Map<String,Object>>> unique=new LinkedHashMap<>();
                        for (int i=0;i<scopes.size();i++) {
                            Scope scope=scopes.get(i);
                            for (var row : BrainOptimize.rows(before.get(Integer.toString(i)))) {
                                if ("WORK_ITEM".equals(scope.table())) CollaborationDbUtils.update(conn,"UPDATE WORK_ITEM SET LINK_TOPIC_ID = ?"+OWNED+" AND ITEM_ID = ?",row.get("LINK_TOPIC_ID"),oid,type,row.get("ITEM_ID"));
                                else unique.computeIfAbsent(scope.table(),k -> new LinkedHashMap<>()).put(BrainTopicReviewProfiles.hash(row),row);
                            }
                        }
                        for (var table : unique.entrySet()) BrainTopicChangeUtils.insertRows(conn,table.getKey(),new ArrayList<>(table.getValue().values()));
                    }
                    var reopened=BrainReviewUtils.reopenReview(user,reviewId);
                    List<String> removed=new ArrayList<>();
                    if ("new_topic".equals(review.get("kind")) && resolution.get("topicId") instanceof String id) removed.add(id);
                    reopened.put("removedTopicIds",removed);
                    result.add(reopened);
                }); return result.get(0);
            }
        });
    }

    public static Map<String,Object> recent(User user,int days,int limit,int offset) {
        if (days<1 || days>180 || limit<1 || limit>100 || offset<0) throw new IllegalArgumentException("Invalid decision pagination");
        var owner=CollaborationDbUtils.ownerOf(user);
        String where=OWNED+" AND RESOLVED_BY = ? AND STATUS <> ? AND RESOLVED_AT >= ?";
        Object[] params={owner.getValue0(),owner.getValue1(),"you","open",Timestamp.valueOf(java.time.LocalDateTime.ofInstant(Instant.now().minus(Duration.ofDays(days)), java.time.ZoneOffset.UTC))};
        List<Map<String,Object>> items=CollaborationDbUtils.query(CollaborationDbUtils.page("SELECT REVIEW_ID, KIND, RESOLVED_AT, RESOLUTION_JSON FROM BRAIN_REVIEW"+where+" ORDER BY RESOLVED_AT DESC, REVIEW_ID",limit,offset),rs -> {
            var resolution=map(CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs,"RESOLUTION_JSON")));
            Map<String,Object> row=new LinkedHashMap<>(); row.put("id",rs.getString("REVIEW_ID")); row.put("kind",rs.getString("KIND")); row.put("at",CollaborationDbUtils.getTimestamp(rs,"RESOLVED_AT"));
            row.put("summary",resolution.getOrDefault("summary","Answered a Brain question")); row.put("canUndo",Boolean.TRUE.equals(resolution.get("applied"))); return row;
        },params);
        return Map.of("items",items,"total",CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_REVIEW"+where,params));
    }
    @SuppressWarnings("unchecked") private static Map<String,Object> map(Object value) { return value instanceof Map<?,?> m ? (Map<String,Object>)m : Map.of(); }
}
