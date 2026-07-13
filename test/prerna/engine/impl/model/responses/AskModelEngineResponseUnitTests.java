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
package prerna.engine.impl.model.responses;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class AskModelEngineResponseUnitTests {
    private AskModelEngineResponse<String> abs;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        abs = mock(AskModelEngineResponse.class, Mockito.withSettings()
            .useConstructor("response", 1, 1)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }

    @Test
    void basicTest() {
        abs.setMessageId("messageId");
        abs.setRoomId("roomId");

        assertEquals("response", abs.getResponse());
        assertEquals("messageId", abs.getMessageId());
        assertEquals("roomId", abs.getRoomId());
        assertEquals("CHAT", abs.getMessageType());
        assertEquals(1, (int) abs.getNumberOfTokensInPrompt());
        assertEquals(1, (int) abs.getNumberOfTokensInResponse());

        assertEquals("messageId", abs.toMap().get("messageId"));
        assertEquals("roomId", abs.toMap().get("roomId"));
        assertEquals("CHAT", abs.toMap().get("messageType"));
    }

    @Test
    void messageTypeNotString() {
        Map<String, Object> map = new HashMap();
        map.put(
            "response", (List)(new ArrayList(){{
                add(new HashMap<String, Object>(){{
                    put("key","value");
                }});
            }})
        );
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType",0);
        
        ClassCastException e = assertThrows(ClassCastException.class, () -> {
            abs.fromMap(map);
        });
        assertNotNull(e.getMessage());
    }

    @Test
    void fromMapToolListResponse() {
        Map<String, Object> map = new HashMap();
        map.put(
            "response", (List)(new ArrayList(){{
                add(new HashMap<String, Object>(){{
                    put("key","value");
                }});
            }})
        );
        map.put("numberOfTokensInPrompt",1);
        map.put("numberOfTokensInResponse",1);
        map.put("messageType","TOOL");

        AskModelEngineResponse ans = abs.fromMap(map);

        assertNotNull(ans);
        assertEquals("[{key=value}]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void toolNotValid() {
        Map<String, Object> map = new HashMap();
        map.put("response", (List)(new ArrayList(){{}}));
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","TOOL");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            abs.fromMap(map);
        });
        assertEquals("Tool list is empty or not valid", e.getMessage());
    }

    @Test
    void responseNotList() {
        Map<String, Object> map = new HashMap();
        map.put("response", 0);
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","TOOL");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            abs.fromMap(map);
        });
        assertEquals("Expected a List response for Tool messageType", e.getMessage());
    }

    @Test
    void fromMapChatMessageType() {
        Map<String, Object> map = new HashMap();
        map.put("response", "response");
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","CHAT");

        AskModelEngineResponse ans = abs.fromMap(map);

        assertNotNull(ans);
        assertEquals("response", ans.getResponse());
    }

    @Test
    void fromMapReadsMetadata() {
        Map<String, Object> metadata = new HashMap<String, Object>(){{
            put("key","value");
            put("nested", new HashMap<String, Object>(){{
                put("inner", 42);
            }});
        }};

        Map<String, Object> map = new HashMap();
        map.put("response", "response");
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","CHAT");
        map.put("metadata", metadata);

        AskModelEngineResponse ans = abs.fromMap(map);

        assertNotNull(ans);
        assertEquals(metadata, ans.getMetadata());
        // round-trips back out through toMap()
        assertEquals(metadata, ans.toMap().get("metadata"));
    }

    @Test
    void fromMapNoMetadata() {
        Map<String, Object> map = new HashMap();
        map.put("response", "response");
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","CHAT");

        AskModelEngineResponse ans = abs.fromMap(map);

        assertNotNull(ans);
        assertEquals(null, ans.getMetadata());
        // omitted from toMap() when absent
        assertEquals(false, ans.toMap().containsKey("metadata"));
    }

    @Test
    void nonStringResponseForChatType() {
        Map<String, Object> map = new HashMap();
        map.put("response", 0);
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","CHAT");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            abs.fromMap(map);
        });
        assertEquals("Expected a String response for Chat messageType", e.getMessage());
    }

    @Test
    void unsupportedMessageType() {
        Map<String, Object> map = new HashMap();
        map.put("response", 0);
        map.put("numberOfTokensInPrompt",0);
        map.put("numberOfTokensInResponse",0);
        map.put("messageType","FOO");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            abs.fromMap(map);
        });
        assertEquals("Unsupported message type: FOO", e.getMessage());
    }

    @Test
    void fromObject() {
        Map<String, Object> map = new HashMap();
        map.put("response", "response");
        map.put("numberOfTokensInPrompt", 0);
        map.put("numberOfTokensInResponse", 0);
        map.put("messageType", "CHAT");

        AskModelEngineResponse ans = abs.fromObject(map);

        assertNotNull(ans);
        assertEquals("response", ans.getResponse());
    }
}
