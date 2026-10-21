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
 *
 * 	You should have received a copy of the GNU General Public License
 * 	along with this program; if not, write to the Free Software
 * 	Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *******************************************************************************/
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.Insight;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import prerna.util.AssetUtility;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class BrainTopicSourcesUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	private final Map<String, Map<String, Object>> rooms = new LinkedHashMap<>();
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_topic_sources_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil).getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					columns.add(column.getValue0() + " " + column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		AccessToken token = new AccessToken();
		token.setId("brief-owner");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		logs = mockStatic(ModelInferenceLogsUtils.class);
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(eq(ownerId), anyList())).thenAnswer(call -> {
			List<Map<String, Object>> found = new ArrayList<>();
			for (Object roomId : (List<?>) call.getArgument(1)) {
				if (rooms.containsKey(roomId)) {
					found.add(rooms.get(roomId));
				}
			}
			return found;
		});
	}

	@AfterEach
	void tearDown() throws Exception {
		logs.close();
		registry.close();
		connection.close();
	}

    @TempDir Path temp;
    private Insight insight() {
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        when(insight.getInsightFolder()).thenReturn(temp.toString());
        return insight;
    }
    private Map<String,Object> note(String topic, String id, String text, Integer version) {
        return BrainTopicSources.save(insight(),topic,id,"Reference","note",text,null,null,version,null);
    }
    private int sourceCount(String topic) {
        return ((List<?>) BrainTopicSources.list(user,topic).get("sources")).size();
    }
    @Test void ownsSourcesAndPreservesImmutableVersions() throws Exception {
        topic("alpha"); topic("beta");
        Map<String,Object> first=note("alpha",null,"Original agreement",null);
        String id=(String) first.get("sourceId");
        assertEquals("ready",first.get("status"));
        assertEquals(1,sourceCount("alpha"));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.get(user,"beta",id,null));
        assertThrows(IllegalArgumentException.class,()->note("alpha",id,"Lost edit",2));
        note("alpha",id,"Updated agreement",1);
        assertEquals("Original agreement",BrainTopicSources.get(user,"alpha",id,1).get("content"));
        assertEquals("Updated agreement",BrainTopicSources.get(user,"alpha",id,null).get("content"));
        AccessToken otherToken=new AccessToken(); otherToken.setId("other"); otherToken.setProvider(AuthProvider.NATIVE);
        User other=new User(); other.setAccessToken(otherToken);
        assertTrue(((List<?>)BrainTopicSources.list(other,null).get("sources")).isEmpty());
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.get(other,"alpha",id,null));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.link(other,"alpha",id,false));
    }
    @Test void sharesByReferenceAndUnlinkPreservesOtherTopics() throws Exception {
        topic("alpha");topic("beta");
        String id=(String)note("alpha",null,"Shared facts",null).get("sourceId");
        BrainTopicSources.link(user,"beta",id,false);
        BrainTopicSources.link(user,"beta",id,false);
        assertEquals(1,sourceCount("beta"));
        BrainTopicSources.link(user,"alpha",id,true);
        assertEquals(0,sourceCount("alpha"));assertEquals(1,sourceCount("beta"));
        assertEquals(1,((List<?>)BrainTopicSources.list(user,null).get("sources")).size());
    }
    @Test void roomScopeExcludesUnlinkedDismissedAndArchivedTopics() throws Exception {
        room("chat",Map.of());topic("alpha");topic("beta");
        roomTopic("alpha","linked","you");roomTopic("beta","suggested","brain");
        String allowed=(String)note("alpha",null,"Approved budget is 42",null).get("sourceId");
        String hidden=(String)note("beta",null,"Private budget is 99",null).get("sourceId");
        assertEquals(1,((List<?>)BrainTopicSources.search(user,"chat","budget",null).get("sources")).size());
        assertEquals("Approved budget is 42",BrainTopicSources.readForChat(user,"chat",allowed,null,null).get("content"));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.readForChat(user,"chat",hidden,null,null));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.search(user,"chat",null,"beta"));
        BrainTopicUtils.setTopicStatus(user,"alpha","archived");
        assertTrue(((List<?>)BrainTopicSources.search(user,"chat",null,null).get("sources")).isEmpty());
        assertEquals(1,sourceCount("alpha"));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.link(user,"alpha",hidden,false));
    }
    @Test void mergesAndDeletesLinksWithUndoWithoutDeletingSharedSources() throws Exception {
        topic("alpha");topic("beta");
        String shared=(String)note("alpha",null,"Shared",null).get("sourceId");
        BrainTopicSources.link(user,"beta",shared,false);
        String only=(String)note("alpha",null,"Alpha only",null).get("sourceId");
        Map<String,Object> merged=BrainTopicUtils.mergeTopics(user,"alpha","beta");
        assertEquals(2,sourceCount("beta"));
        assertEquals("beta",BrainTopicSources.get(user,"alpha",only,1).get("topicId"));
        BrainTopicChangeUtils.undo(user,(String)merged.get("changeId"));
        assertEquals(2,sourceCount("alpha"));assertEquals(1,sourceCount("beta"));
        Map<String,Object> deleted=BrainTopicUtils.deleteTopic(user,"alpha");
        assertEquals(1,sourceCount("beta"));
        BrainTopicChangeUtils.undo(user,(String)deleted.get("changeId"));
        assertEquals("Alpha only",BrainTopicSources.get(user,"alpha",only,null).get("content"));
    }
    @Test void rejectsUnreadableDocumentsAndReportsExtractionTruncation() throws Exception {
        topic("alpha");Files.writeString(temp.resolve("empty.txt"),"  ");
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.save(insight(),"alpha",null,"Blank","document",null,"empty.txt",null,null,null));
        Path longFile=temp.resolve("long.txt");Files.writeString(longFile,"x".repeat(BrainTopicSources.MAX_CONTENT+100));
        var text=BrainTopicSources.extract(longFile,"long.txt");
        assertTrue(text.truncated());assertEquals(BrainTopicSources.MAX_CONTENT,text.text().length());
    }

    @Test void historicalMergeAliasNeverBypassesCurrentSourceLink() throws Exception {
        topic("alpha");topic("beta");topic("other");
        String id=(String)note("alpha",null,"Historical citation",null).get("sourceId");
        BrainTopicSources.link(user,"other",id,false);
        BrainTopicUtils.mergeTopics(user,"alpha","beta");
        assertEquals("beta",BrainTopicSources.get(user,"alpha",id,1).get("topicId"));
        BrainTopicSources.link(user,"beta",id,true);
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.get(user,"alpha",id,1));
        assertEquals("Historical citation",BrainTopicSources.get(user,"other",id,1).get("content"));
    }

    @Test void importsToDurableStorageAndRejectsTraversalAndSymlinks() throws Exception {
        topic("alpha");
        Path input=temp.resolve("uploaded.md");Files.writeString(input,"Durable document contents");
        Path storage=Files.createDirectory(temp.resolve("user-storage"));
        Insight insight=insight();
        try(MockedStatic<AssetUtility> assets=mockStatic(AssetUtility.class)) {
            assets.when(()->AssetUtility.getRootFolderPath(insight,AssetUtility.USER_SPACE_KEY,true)).thenReturn(storage.toString());
            Map<String,Object> saved=BrainTopicSources.save(insight,"alpha",null,"Spec","document",null,"uploaded.md",null,null,null);
            String id=(String)saved.get("sourceId");
            Files.delete(input);
            assertEquals("Durable document contents",BrainTopicSources.get(user,"alpha",id,null).get("content"));
            assertEquals("Durable document contents",Files.readString(storage.resolve(".collaboration-sources").resolve(id).resolve("1/original")));
        }
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.safeInput(temp,"../outside.txt"));
        Path outside=Files.createTempFile("source-outside",".txt");
        try {
            Files.createSymbolicLink(temp.resolve("escape.txt"),outside);
            assertThrows(IllegalArgumentException.class,()->BrainTopicSources.safeInput(temp,"escape.txt"));
        } finally {Files.deleteIfExists(outside);}
    }
    @Test void readsLaterPassagesAndFindsKeywordsFromNaturalQuestions() throws Exception {
        room("chat",Map.of());topic("alpha");roomTopic("alpha","linked","you");
        String text="Intro "+"x".repeat(14_000)+" budget deadline is Friday";
        String id=(String)note("alpha",null,text,null).get("sourceId");
        Map<String,Object> first=BrainTopicSources.readForChat(user,"chat",id,null,null,0);
        assertEquals(12000,((String)first.get("content")).length());assertEquals(12000,first.get("nextOffset"));
        Map<String,Object> second=BrainTopicSources.readForChat(user,"chat",id,null,null,12000);
        assertTrue(((String)second.get("content")).contains("deadline is Friday"));assertEquals(false,second.get("hasMore"));
        assertTrue(((Map<?,?>)second.get("citation")).get("locator").toString().contains("12001"));
        assertEquals(1,((List<?>)BrainTopicSources.search(user,"chat","What is the budget deadline?",null).get("sources")).size());
    }
    @Test void assistantCannotWidenTopicScopeWithProviderOrMailTools() throws Exception {
        room("chat",Map.of());topic("alpha");topic("beta");roomTopic("alpha","linked","you");
        Insight in=insight();when(in.getRoomId()).thenReturn("chat");
        thread("legacy-confirmed");threadTopic("legacy-confirmed","alpha",null);
        BrainTopicSources.requireChatThread(user,"chat","legacy-confirmed");
        // All three attempts must fail before any provider request, external read, or Pixel execution.
        assertFalse(CollaborationAgentTools.execute("SearchFiles",Map.of("query","private"),in).isSuccess());
        assertFalse(CollaborationAgentTools.execute("SearchMail",Map.of("topicId","beta"),in).isSuccess());
        assertFalse(CollaborationAgentTools.execute("ReadThread",Map.of("threadId","unrelated"),in).isSuccess());
        assertFalse(CollaborationAgentTools.execute("ListTopics",Map.of("topicId","beta"),in).isSuccess());
        assertTrue(CollaborationAgentTools.execute("SearchTopicSources",Map.of(),in).isSuccess());
    }
    @Test void roomFileListingAndImportRecheckOwnership() throws Exception {
        room("chat",Map.of());topic("alpha");
        Path roomFolder=Files.createDirectory(temp.resolve("room"));
        Files.writeString(roomFolder.resolve("room-note.md"),"Downloaded M365 file copy");
        Files.writeString(roomFolder.resolve(".private.txt"),"Hidden");
        Room loaded=mock(Room.class);when(loaded.getRoomFolderPath()).thenReturn(roomFolder.toString());
        Insight in=insight();Path storage=Files.createDirectory(temp.resolve("assets"));
        try(var rooms=mockStatic(prerna.engine.impl.model.RoomUtils.class);var assets=mockStatic(AssetUtility.class)) {
            rooms.when(()->prerna.engine.impl.model.RoomUtils.getOrLoadRoom("chat",in)).thenReturn(loaded);
            assets.when(()->AssetUtility.getRootFolderPath(in,AssetUtility.USER_SPACE_KEY,true)).thenReturn(storage.toString());
            var files=(List<?>)BrainTopicSources.roomFiles(in,"chat").get("files");
            assertEquals(1,files.size());assertEquals("room-note.md",((Map<?,?>)files.getFirst()).get("path"));
            var saved=BrainTopicSources.save(in,"alpha",null,"Room doc","document",null,"room-note.md",null,null,"chat");
            assertEquals("snapshot",saved.get("mode"));assertEquals("Downloaded M365 file copy",saved.get("content"));
            assertThrows(IllegalArgumentException.class,()->BrainTopicSources.roomFiles(in,"someone-elses-room"));
            assertThrows(IllegalArgumentException.class,()->BrainTopicSources.save(in,"alpha",null,"No","document",null,"room-note.md",null,null,"someone-elses-room"));
        }
    }

    @Test void contextBoundaryTracksUnlinksAndSourceVersions() throws Exception {
        Room room=room("chat",Map.of()); topic("alpha"); roomTopic("alpha","linked","you");
        String id=(String)note("alpha",null,"Old facts",null).get("sourceId");
        List<prerna.engine.impl.model.message.AbstractMessage> history=new ArrayList<>();
        when(room.getMessages()).thenReturn(history);
        var first=prerna.engine.impl.model.message.InputMessage.builder(room).withText("Question").build();
        BrainTopicSources.markContextBoundary(user,room,first);history.add(first);
        assertEquals(true,first.getOrnament("topicSourcesContextStart"));
        var same=prerna.engine.impl.model.message.InputMessage.builder(room).withText("Follow up").build();
        BrainTopicSources.markContextBoundary(user,room,same);history.add(same);
        assertNull(same.getOrnament("topicSourcesContextStart"));
        note("alpha",id,"New facts",1);
        var changed=prerna.engine.impl.model.message.InputMessage.builder(room).withText("Current facts?").build();
        BrainTopicSources.markContextBoundary(user,room,changed);history.add(changed);
        assertEquals(true,changed.getOrnament("topicSourcesContextStart"));
        BrainTopicSources.link(user,"alpha",id,true);
        var unlinked=prerna.engine.impl.model.message.InputMessage.builder(room).withText("Next").build();
        BrainTopicSources.markContextBoundary(user,room,unlinked);
        assertEquals(true,unlinked.getOrnament("topicSourcesContextStart"));
    }

    @Test void storesExternalReferenceWithoutFetchingAndRejectsUnsafeSchemes() throws Exception {
        topic("alpha");
        Map<String,Object> link=BrainTopicSources.save(insight(),"alpha",null,"Provider doc","link",null,null,"https://example.invalid/document",null,null);
        assertEquals("reference",link.get("status"));assertEquals("",link.get("content"));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.validateUrl("file:///etc/passwd"));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.validateUrl("https://user:pass@example.com/doc"));
        assertThrows(IllegalArgumentException.class,()->BrainTopicSources.validateUrl("javascript:alert(1)"));
    }

	private Room room(String id, Map<String, Object> options) {
		Room room = mock(Room.class);
		when(room.getId()).thenReturn(id);
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(options);
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("ROOM_ID", id);
		row.put("PROJECT_ID", CollaborationUtils.COLLABORATION_PROJECT_ID);
		row.put("OPTIONS", CollaborationDbUtils.toJson(options));
		rooms.put(id, row);
		return room;
	}

	private void topic(String id) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) VALUES (?, ?, ?, ?, 'active')",
				ownerId, ownerType, id, id);
	}

	private void thread(String id) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE) VALUES (?, ?, ?, 'email')", ownerId,
				ownerType, id);
	}

	private void threadTopic(String threadId, String topicId, String source) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE) VALUES (?, ?, ?, ?, ?)",
				ownerId, ownerType, threadId, topicId, source);
	}

	private void roomTopic(String topicId, String state, String origin) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES (?, ?, 'chat', ?, ?, ?)", ownerId, ownerType, topicId, state, origin);
	}

	private void roomThread(String threadId, String state) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, THREAD_ID, STATE) "
				+ "VALUES (?, ?, 'chat', ?, ?)", ownerId, ownerType, threadId, state);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}
}
