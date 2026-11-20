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

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class NerModelEngineResponseUnitTests {
    private NerModelEngineResponse reactor;

    @Test
    void test() {
        Map<String, Object> map = new HashMap(){{put("key", "value");}};

        NerModelEngineResponse ans = new NerModelEngineResponse(map, 1, 2);
        ans.setMessageId("messageId");
        ans.setRoomId("roomId");

        assertNotNull(ans);
        assertEquals("{key=value}", ans.getResponse().toString());
        assertEquals("messageId", ans.getMessageId());
        assertEquals("roomId", ans.getRoomId());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJson() {
        JSONObject obj = new JSONObject();
        obj.put("mask_values", new JSONObject(){{put("key","value");}});
        obj.put("entities", new JSONArray(){{put("string");}});
        obj.put("raw_output", new JSONArray(){{
            put(new JSONObject(){{
                put("start", 0);
                put("end", 1);
                put("text", "text");   
                put("label", "label");  
                put("score", 5.0);  
            }});
        }});
        obj.put("output", "output");
        obj.put("input", "input");
        obj.put("status", "status");
        obj.put("message", "message");

        NerModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("{mask_values={key=value}, output=output, input=input, entities=[string], raw_output=[{score=5.0, start=0, end=1, text=text, label=label}], message=message, status=status}", ans.getResponse().toString());
        assertEquals(0, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(0, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonEmptyObj() {
        JSONObject obj = new JSONObject();

        NerModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("{message=, status=success}", ans.getResponse().toString());
        assertEquals(0, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(0, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonNullObj() {
        NerModelEngineResponse ans = reactor.fromJson(null);

        assertNotNull(ans);
        assertEquals("{message=Null response from model request, status=error}", ans.getResponse().toString());
        assertEquals(0, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(0, (int) ans.getNumberOfTokensInResponse());
    }
}
