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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class BrainReviewWorkflowTest {
    Connection connection;
    MockedStatic<SystemEngineRegistry> registry;
    MockedStatic<User> users;
    User user=mock(User.class);
    @BeforeEach void setup() throws Exception {
        connection=DriverManager.getConnection("jdbc:h2:mem:review-"+UUID.randomUUID());
        var queryUtil=SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
        try(var s=connection.createStatement()) { for(var table:new CollaborationOwlCreator(queryUtil).getDBSchema()) s.execute("CREATE TABLE "+table.getValue0()+" ("+String.join(", ",table.getValue1().stream().map(c->c.getValue0()+" "+c.getValue1()).toList())+")"); }
        IRDBMSEngine engine=mock(IRDBMSEngine.class);
        when(engine.getQueryUtil()).thenReturn(queryUtil); when(engine.getConnection()).thenReturn(connection);
        when(engine.getPreparedStatement(anyString())).thenAnswer(call->connection.prepareStatement(call.getArgument(0)));
        registry=mockStatic(SystemEngineRegistry.class); registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
        users=mockStatic(User.class); users.when(()->User.getPrimaryUserIdAndTypePair(user)).thenReturn(Pair.with("owner","NATIVE"));
        topic("a","Northwind Migration"); topic("b","Migration Delivery");
        sql("INSERT INTO BRAIN_PERSON (OWNER_ID,OWNER_TYPE,PERSON_ID,DISPLAY_NAME) VALUES ('owner','NATIVE','p','Frequent Sender')");
    }
    @AfterEach void close() throws Exception { users.close();registry.close();connection.close(); }
    void sql(String sql,Object...args) throws Exception { try(var ps=connection.prepareStatement(sql)){for(int i=0;i<args.length;i++)ps.setObject(i+1,args[i]);ps.executeUpdate();} }
    void topic(String id,String name)throws Exception {sql("INSERT INTO BRAIN_TOPIC (OWNER_ID,OWNER_TYPE,TOPIC_ID,NAME,STATUS,KIND) VALUES ('owner','NATIVE',?,?,'active','internal')",id,name);}
    void thread(String id,int days)throws Exception {sql("INSERT INTO BRAIN_THREAD (OWNER_ID,OWNER_TYPE,THREAD_ID,SUBJECT,SOURCE,LAST_MESSAGE_AT) VALUES ('owner','NATIVE',?,?,'email',?)",id,"ReviewCase "+id,Timestamp.from(Instant.now().minus(Duration.ofDays(days))));}
    Map<String,Object> question(String kind,String refType,String ref,Map<String,Object> data) {return map(BrainReviewUtils.addReview("owner","NATIVE",kind,refType,ref,"test:"+kind,data).get("review"));}
    @SuppressWarnings("unchecked") static Map<String,Object> map(Object o){return (Map<String,Object>)o;}
    @SuppressWarnings("unchecked") static List<Map<String,Object>> rows(Object o){return (List<Map<String,Object>>)o;}
    int count(String table) {return CollaborationDbUtils.count("SELECT COUNT(*) FROM "+table+" WHERE OWNER_ID = ? AND OWNER_TYPE = ?","owner","NATIVE");}

    @Test void healthUsesStoredEligibleMailAndManualAnswersImproveCoverageOnly()throws Exception {
        for(int i=0;i<9;i++)thread("u"+i,1);
        for(String id:List.of("auto","manual","onboard"))thread(id,1);
        sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID,OWNER_TYPE,THREAD_ID,TOPIC_ID,SOURCE,CLASSIFIER_VERSION) VALUES ('owner','NATIVE','auto','a','confirmed','model')");
        sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID,OWNER_TYPE,THREAD_ID,TOPIC_ID,SOURCE) VALUES ('owner','NATIVE','manual','a','you')");
        sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID,OWNER_TYPE,THREAD_ID,TOPIC_ID,SOURCE,CLASSIFIER_VERSION) VALUES ('owner','NATIVE','onboard','a','confirmed',?)",BrainTopicOnboarding.VERSION);
        thread("old",40);thread("muted",1);sql("UPDATE BRAIN_THREAD SET MUTED = TRUE WHERE THREAD_ID = 'muted'");
        thread("bulk",1);sql("UPDATE BRAIN_THREAD SET AUTOMATED = TRUE WHERE THREAD_ID = 'bulk'");
        var health=BrainReviewHealth.health(user,7);assertEquals(12,health.get("eligibleMailThreads"));assertEquals(9,health.get("unfiledThreads"));assertEquals(1,health.get("automaticallyFiledThreads"));
        BrainReviewHealth.ensureUnassigned(user);BrainReviewHealth.ensureUnassigned(user);assertEquals(10,count("BRAIN_REVIEW"));
        var q=question("unassigned","thread","u0",Map.of());
        BrainReviewDecisions.answer(user,(String)q.get("id"),"accept","a");
        var after=BrainReviewHealth.health(user,7);assertEquals(8,after.get("unfiledThreads"));assertEquals(1,after.get("automaticallyFiledThreads"));assertTrue((Double)after.get("filedPercent")>(Double)health.get("filedPercent"));
    }

    @Test void proposalsDoNotApplyChangesAndDoNotDuplicate()throws Exception {
        thread("one",1);thread("two",1);
        int topics=count("BRAIN_TOPIC"),people=count("BRAIN_TOPIC_PERSON");
        var scores=new BrainClassifier.Scores(Map.of("a",.92,"b",.07,"other",.01),0,0,0,Map.of());
        assertEquals(2,BrainOptimize.proposeFilings(user,List.of("one","two"),id->scores,(d,t)->{}));
        assertEquals(0,BrainOptimize.proposeFilings(user,List.of("one","two"),id->scores,(d,t)->{}));
        var candidate=Map.<String,Object>of("name","New Product Launch","kind","internal","threadIds",List.of("one","two"),"memberIds",List.of("p"));
        assertEquals(1,BrainOptimize.proposeTopics(user,List.of(candidate)));assertEquals(0,BrainOptimize.proposeTopics(user,List.of(candidate)));
        assertEquals(topics,count("BRAIN_TOPIC"));assertEquals(people,count("BRAIN_TOPIC_PERSON"));assertEquals(0,count("BRAIN_THREAD_TOPIC"));assertEquals(0,count("WORK_ITEM"));
    }

    @Test void filingAnswerUndoAndLaterEditProtectionAreDurable()throws Exception {
        thread("one",1);
        sql("INSERT INTO WORK_ITEM (OWNER_ID,OWNER_TYPE,ITEM_ID,THREAD_ID,TITLE,STATUS) VALUES ('owner','NATIVE','work','one','Keep my task','waiting')");
        var q=question("unassigned","thread","one",Map.of());String id=(String)q.get("id");
        BrainReviewDecisions.answer(user,id,"accept","a");
        assertEquals("waiting",CollaborationDbUtils.queryOne("SELECT STATUS FROM WORK_ITEM",rs->rs.getString(1)));
        assertEquals(1,BrainReviewDecisions.recent(user,7,20,0).get("total"));
        assertFalse(map(BrainReviewUtils.getReview(user,id).get("resolution")).containsKey("before"));
        BrainReviewDecisions.undo(user,id);assertEquals(0,count("BRAIN_THREAD_TOPIC"));assertEquals("open",BrainReviewUtils.getReview(user,id).get("status"));
        BrainReviewDecisions.answer(user,id,"accept","a");
        BrainThreadUtils.linkThreadTopic(user,"one","b",false,false);
        assertThrows(IllegalArgumentException.class,()->BrainReviewDecisions.undo(user,id));assertEquals(2,count("BRAIN_THREAD_TOPIC"));
    }

    @Test void invalidAnswerRollsBackAndAnotherOwnerCannotAnswer()throws Exception {
        thread("one",1);var q=question("unassigned","thread","one",Map.of());String id=(String)q.get("id");
        assertThrows(IllegalArgumentException.class,()->BrainReviewDecisions.answer(user,id,"accept","missing"));assertEquals("open",BrainReviewUtils.getReview(user,id).get("status"));
        users.when(()->User.getPrimaryUserIdAndTypePair(user)).thenReturn(Pair.with("someone-else","NATIVE"));
        assertThrows(IllegalArgumentException.class,()->BrainReviewDecisions.answer(user,id,"accept","a"));assertEquals(0,BrainReviewDecisions.recent(user,7,20,0).get("total"));
    }

    @Test void quietTopicAndMembershipAnswersCanBeUndone()throws Exception {
        var quiet=question("quiet_topic","topic","a",Map.of("text","Archive quiet topic?"));String q=(String)quiet.get("id");
        BrainReviewDecisions.answer(user,q,"accept",null);assertEquals("archived",BrainTopicUtils.getTopic(user,"a").get("status"));BrainReviewDecisions.undo(user,q);assertEquals("active",BrainTopicUtils.getTopic(user,"a").get("status"));
        var person=question("unknown_sender","person","p",Map.of("text","Where does this sender belong?"));String p=(String)person.get("id");
        BrainReviewDecisions.answer(user,p,"dismiss",null);BrainReviewDecisions.undo(user,p);
        BrainReviewDecisions.answer(user,p,"accept","a");assertEquals(1,count("BRAIN_TOPIC_PERSON"));BrainReviewDecisions.undo(user,p);assertEquals(0,count("BRAIN_TOPIC_PERSON"));
    }

    @Test void aQuietTopicThatReceivesMailCannotBeArchivedFromAnOldQuestion() throws Exception {
        var quiet=question("quiet_topic","topic","a",Map.of());
        thread("new-mail",1);BrainThreadUtils.linkThreadTopic(user,"new-mail","a",true,false);
        assertThrows(IllegalArgumentException.class,()->BrainReviewDecisions.answer(user,(String)quiet.get("id"),"accept",null));
        assertEquals("active",BrainTopicUtils.getTopic(user,"a").get("status"));
        assertEquals("open",BrainReviewUtils.getReview(user,(String)quiet.get("id")).get("status"));
    }

    @Test void acceptingNewTopicCreatesOnlyAfterAnswerAndUndoRestoresThreads()throws Exception {
        thread("one",1);thread("two",1);
        var q=question("new_topic","thread","one",Map.of("name","Product Launch","kind","internal","text","New topic: Product Launch","threadIds",List.of("one","two"),"memberIds",List.of("p")));String id=(String)q.get("id");
        BrainReviewDecisions.answer(user,id,"accept",null);assertEquals(3,count("BRAIN_TOPIC"));assertEquals(2,count("BRAIN_THREAD_TOPIC"));
        BrainReviewDecisions.undo(user,id);assertEquals(2,count("BRAIN_TOPIC"));assertEquals(0,count("BRAIN_THREAD_TOPIC"));assertEquals(0,count("BRAIN_TOPIC_PERSON"));
    }

    @Test void memoryAnswerUndoRestoresTheReplacedMemoryToo()throws Exception {
        sql("INSERT INTO BRAIN_MEMORY (OWNER_ID,OWNER_TYPE,MEMORY_ID,KIND,TEXT,STATE,ORIGIN,CONFIRMED) VALUES ('owner','NATIVE','old','fact','Old fact','active','brain',FALSE)");
        sql("INSERT INTO BRAIN_MEMORY (OWNER_ID,OWNER_TYPE,MEMORY_ID,KIND,TEXT,STATE,ORIGIN,CONFIRMED,REPLACES_ID) VALUES ('owner','NATIVE','new','fact','Corrected fact','suggested','brain',FALSE,'old')");
        BrainReviewHealth.ensureMemories(user);var q=rows(BrainReviewUtils.listReview(user,"open",20,0).get("items")).get(0);String id=(String)q.get("id");
        BrainReviewDecisions.answer(user,id,"accept",null);assertEquals("superseded",BrainMemoryUtils.find("owner","NATIVE","old").state());
        BrainReviewDecisions.undo(user,id);assertEquals("active",BrainMemoryUtils.find("owner","NATIVE","old").state());assertEquals("suggested",BrainMemoryUtils.find("owner","NATIVE","new").state());
    }
    @Test void explicitOwnerCorrectionIsProposalOnlyAndUndoRestoresTheOriginalFiling() throws Exception {
        thread("one",1);BrainThreadUtils.linkThreadTopic(user,"one","a",true,false);
        assertFalse(BrainOptimize.candidates(user).contains("one"));
        var review=BrainReviewDecisions.requestCorrection(user,"one");String id=(String)review.get("id");
        var overview=BrainReviewHealth.overview(user,50,0);
        var visible=rows(map(overview.get("pending")).get("items")).stream().filter(row -> id.equals(row.get("id"))).findFirst().orElseThrow();
        assertEquals("ReviewCase one",map(visible.get("data")).get("subject"));
        assertEquals("Northwind Migration",rows(map(visible.get("data")).get("currentTopics")).get(0).get("name"));
        assertEquals(1,map(overview.get("pendingCounts")).get("wrong_topic"));
        assertEquals("a",BrainReviewHealth.links(user).get("one").get(0).get("topicId"));
        BrainReviewDecisions.answer(user,id,"accept","b");assertEquals("b",BrainReviewHealth.links(user).get("one").get(0).get("topicId"));
        BrainReviewDecisions.undo(user,id);assertEquals("a",BrainReviewHealth.links(user).get("one").get(0).get("topicId"));assertEquals(0,count("BRAIN_THREAD_TOPIC_REJECTION"));
    }

    @Test void mergeSuggestionsDoNotMergeUntilAcceptedAndUseExistingUndo() throws Exception {
        var topics=BrainOptimize.topics(user);
        var cards=topics.stream().map(t->new BrainTopicAreas.Topic((String)t.get("name"),"Northwind migration delivery",0)).toList();
        var groups=BrainOptimize.mergeGroups((prompt,instructions,params)-> {
            assertEquals(BrainOptimize.MERGE_INSTRUCTIONS,instructions);
            return "{\"areas\":[{\"name\":\"Northwind Migration\",\"about\":\"Both cover the same migration delivery work\",\"topics\":[\"t1\",\"t2\"]}]}";
        },cards);
        assertEquals(1,BrainOptimize.proposeMerges(user,topics,groups));
        assertEquals(2,count("BRAIN_TOPIC"));
        var review=rows(BrainReviewUtils.listReview(user,"open",20,0).get("items")).get(0);String id=(String)review.get("id");
        BrainReviewDecisions.answer(user,id,"accept",null);assertEquals(1,count("BRAIN_TOPIC"));
        BrainReviewDecisions.undo(user,id);assertEquals(2,count("BRAIN_TOPIC"));
    }

    @Test void quietAndFrequentPeopleProposalsAreIdempotentAndRemovedMembershipsStayOut() throws Exception {
        for(int i=0;i<3;i++) {
            String id="sent"+i;thread(id,1);BrainThreadUtils.linkThreadTopic(user,id,"a",true,false);
            sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID,OWNER_TYPE,THREAD_ID,MESSAGE_KEY,SENDER_PERSON_ID) VALUES ('owner','NATIVE',?,?,'p')",id,"msg"+i);
        }
        assertEquals(2,BrainOptimize.proposePeople(user));assertEquals(0,BrainOptimize.proposePeople(user));
        assertEquals(0,count("BRAIN_TOPIC_PERSON"));assertEquals(1,BrainOptimize.proposeQuiet(user));assertEquals(0,BrainOptimize.proposeQuiet(user));
        BrainTopicUtils.setTopicPerson(user,"a","p","removed",null);assertEquals(0,BrainOptimize.proposePeople(user));
    }

    @Test void changedMemoryCannotBeConfirmedFromAnOutdatedQuestion() throws Exception {
        sql("INSERT INTO BRAIN_MEMORY (OWNER_ID,OWNER_TYPE,MEMORY_ID,KIND,TEXT,STATE,ORIGIN,CONFIRMED) VALUES ('owner','NATIVE','learned','fact','Old context','active','assistant',FALSE)");
        BrainReviewHealth.ensureMemories(user);var review=rows(BrainReviewUtils.listReview(user,"open",20,0).get("items")).get(0);String id=(String)review.get("id");
        sql("UPDATE BRAIN_MEMORY SET TEXT = 'New context' WHERE MEMORY_ID = 'learned'");
        assertThrows(IllegalArgumentException.class,()->BrainReviewDecisions.answer(user,id,"accept",null));
        assertEquals("open",BrainReviewUtils.getReview(user,id).get("status"));
        BrainReviewHealth.ensureMemories(user);assertEquals(1,BrainReviewUtils.listReview(user,"open",20,0).get("total"));
        assertEquals(0,BrainReviewDecisions.recent(user,7,20,0).get("total"));
    }

    @Test void pendingAndRecentDecisionsArePagedAndUseUtcWindowBoundaries() throws Exception {
        for(int i=0;i<55;i++) {
            thread("page-"+i,1);
            var q=question("unassigned","thread","page-"+i,Map.of());
            if(i<23) BrainReviewDecisions.answer(user,(String)q.get("id"),"dismiss",null);
        }
        assertEquals(23,BrainReviewDecisions.recent(user,7,20,0).get("total"));
        assertEquals(3,rows(BrainReviewDecisions.recent(user,7,20,20).get("items")).size());
        assertEquals(32,BrainReviewUtils.listReview(user,"open",10,0).get("total"));
        assertEquals(2,rows(BrainReviewUtils.listReview(user,"open",10,30).get("items")).size());
        Timestamp justInside=new Timestamp(CollaborationDbUtils.now().getTime()-Duration.ofDays(7).toMillis()+60000);
        sql("UPDATE BRAIN_REVIEW SET CREATED_AT = ?, RESOLVED_AT = CASE WHEN STATUS <> 'open' THEN ? ELSE NULL END",justInside,justInside);
        assertEquals(23,BrainReviewDecisions.recent(user,7,20,0).get("total"));
        assertEquals(32,BrainReviewHealth.health(user,7).get("questionsWaiting"));
        Timestamp justOutside=new Timestamp(justInside.getTime()-120000);
        sql("UPDATE BRAIN_REVIEW SET CREATED_AT = ?, RESOLVED_AT = CASE WHEN STATUS <> 'open' THEN ? ELSE NULL END",justOutside,justOutside);
        assertEquals(0,BrainReviewDecisions.recent(user,7,20,0).get("total"));
        assertEquals(0,BrainReviewHealth.health(user,7).get("questionsWaiting"));
        assertEquals(32,BrainReviewUtils.listReview(user,"open",10,0).get("total"));
    }

    @Test void reviewTopicDiscoveryIncludesSyncedMailAndAllowsItsOwnRunningJob() throws Exception {
        Timestamp imported = Timestamp.from(Instant.now().minus(Duration.ofDays(2)));
        sql("INSERT INTO COLLAB_JOB (OWNER_ID,OWNER_TYPE,JOB_ID,KIND,STATUS,PARAMS_JSON,STARTED_AT,FINISHED_AT) VALUES ('owner','NATIVE','import','import','done','{\"days\":30}',?,?)", imported, imported);
        sql("INSERT INTO BRAIN_PERSON (OWNER_ID,OWNER_TYPE,PERSON_ID,EMAIL_NORM,RELATIONSHIP) VALUES ('owner','NATIVE','self','alex@owner.example','self')");
        sql("UPDATE BRAIN_PERSON SET EMAIL_NORM = 'avery@cedar.example' WHERE PERSON_ID = 'p'");
        CollaborationSourceUtils.setSourceEnabled("owner","NATIVE","email",true);
        var eligible = new java.util.LinkedHashMap<String,BrainTopicSuggest.Thread>();
        var emails = new java.util.HashMap<String,String>(Map.of("self","alex@owner.example","p","avery@cedar.example"));
        // Discovery drops vocabulary shared by too much of the mailbox; include unrelated mail.
        for (int i=0;i<40;i++) {
            String sender=i<4 ? "p" : "other-"+i;
            emails.putIfAbsent(sender,"sender"+i+"@unrelated.example");
            String id="cedar-"+i, subject=i<4 ? "Cedar Relay deployment planning " + i : "Unrelated message " + i;
            thread(id,0);sql("UPDATE BRAIN_THREAD SET SUBJECT = ? WHERE THREAD_ID = ?",subject,id);
            sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID,OWNER_TYPE,THREAD_ID,MESSAGE_KEY,SENDER_PERSON_ID,DECISION,RECEIVED_AT,BULK,FOLDER) VALUES ('owner','NATIVE',?,?,?,?,?,FALSE,'inbox')",id,"msg-"+id,sender,BrainRulesGate.INGESTED,new Timestamp(CollaborationDbUtils.now().getTime()-3600000));
            for (String person:List.of("self",sender)) sql("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID,OWNER_TYPE,THREAD_ID,PERSON_ID,INCLUDED) VALUES ('owner','NATIVE',?,?,TRUE)",id,person);
            eligible.put(id,new BrainTopicSuggest.Thread(id,subject,java.util.Set.of("self",sender)));
        }
        BrainTopicVotes.Caller caller=(prompt,instructions,parameters)-> {
            var clusters=com.google.gson.JsonParser.parseString(prompt).getAsJsonObject().getAsJsonArray("clusters");
            var answers=new java.util.ArrayList<Map<String,Object>>();
            for (var cluster:clusters) answers.add(Map.of("id",cluster.getAsJsonObject().get("id").getAsString(),"kind","work","name","Cedar Relay","about","Cedar relay deployment planning and delivery."));
            return new com.google.gson.Gson().toJson(Map.of("clusters",answers));
        };
        try (var jobs=mockStatic(CollaborationJobUtils.class,CALLS_REAL_METHODS);var onboarding=mockStatic(BrainTopicOnboarding.class,CALLS_REAL_METHODS)) {
            jobs.when(()->CollaborationJobUtils.anyRunning(anyString(),anyString(),anyString())).thenAnswer(call->!BrainOptimize.KIND.equals(call.getArgument(2)));
            onboarding.when(()->BrainTopicOnboarding.caller(user,"model")).thenReturn(caller);
            var result=BrainTopicOnboarding.propose(user,"model","owner","NATIVE",eligible,"self",emails,java.util.Set.of(),BrainOrgDomains.of(List.of(),"owner.example"),java.util.Set.of(),null,true);
            assertEquals(40,result.diagnostics().get("messages"));
            assertEquals(true,result.diagnostics().get("wide"));
            assertFalse(result.candidates().isEmpty(),result.diagnostics().toString());
            assertEquals(0,count("BRAIN_THREAD_TOPIC"));assertEquals(2,count("BRAIN_TOPIC"));
        }
    }

    @Test void generatedMailboxHasNineNewUnfiledTargetsAndBothFolderKinds() throws Exception {
        String path=System.getProperty("brainReviewFixture");
        org.junit.jupiter.api.Assumptions.assumeTrue(path != null,"Optional generated mailbox smoke test");
        var source=BrainFixtureHeaderSource.of(path);
        var inbox=source.list(user,BrainMailHeaderSource.INBOX,Instant.now().minus(Duration.ofDays(7)),5000);
        assertEquals(9,inbox.stream().filter(row -> String.valueOf(row.get("subject")).startsWith("ReviewCase Unfiled:")).count());
        assertTrue(source.list(user,BrainMailHeaderSource.SENT,Instant.now().minus(Duration.ofDays(7)),5000).stream().anyMatch(row -> "ReviewCase: fixture owner identity".equals(row.get("subject"))));
        assertEquals(2,source.list(user,BrainMailHeaderSource.INBOX,Instant.now().minus(Duration.ofDays(90)),5000).stream().filter(row -> String.valueOf(row.get("subject")).startsWith("ReviewCase Legacy Audit:")).count());
    }

}
