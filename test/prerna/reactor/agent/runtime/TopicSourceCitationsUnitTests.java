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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.json.JSONObject;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;

class TopicSourceCitationsUnitTests {
    @Test void citesOnlyEvidenceActuallyReturnedAndReferencedAndSurvivesPersistence() {
        AgentLoopState state=new AgentLoopState();
        Map<String,Object> citation=Map.of("sourceId","source-a","version",2,"topicId","topic-a",
                "title","Contract","excerpt","Delivery is Friday","locator","paragraph 1","citationId","source-a:v2");
        state.recordTopicEvidence(new JSONObject(Map.of("sources",List.of(Map.of("status","ready","citation",citation)))).toString());
        ResponseMessage response=ResponseMessage.text("Delivery is Friday [[source:source-a:v2]]");
        state.attachTopicCitations(response);
        assertEquals(List.of(citation),response.getOrnament("topicSources"));
        Room room=new Room(); room.setId("citation-room"); response.setRoom(room);
        String json=MessageUtils.toJsonArray(List.of(response));
        var loaded=RoomMessageStore.loadFromPersistedJson(room,json);
        assertEquals("source-a",((Map<?,?>)((List<?>)loaded.getFirst().getOrnament("topicSources")).getFirst()).get("sourceId"));
        ResponseMessage uncited=ResponseMessage.text("A general response");
        state.attachTopicCitations(uncited);assertNull(uncited.getOrnament("topicSources"));
        ResponseMessage invented=ResponseMessage.text("Claim [[source:unread:v1]]");
        state.attachTopicCitations(invented);assertNull(invented.getOrnament("topicSources"));
    }
    @Test void referenceOnlyLinksCannotBecomeEvidenceOfTheirContents() {
        AgentLoopState state=new AgentLoopState();
        state.recordTopicEvidence(new JSONObject(Map.of("status","reference","citation",
                Map.of("citationId","link:v1","sourceId","link","version",1))).toString());
        ResponseMessage response=ResponseMessage.text("Alleged content [[source:link:v1]]");
        state.attachTopicCitations(response);assertNull(response.getOrnament("topicSources"));
    }
}
