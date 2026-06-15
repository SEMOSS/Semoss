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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AskToolModelEngineResponseUnitTests {
    private AskToolModelEngineResponse reactor;

    @Test
    void test() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", "id");
        map.put("name", "name");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("key", "value");
        map.put("arguments", map2);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);

        
        AskToolModelEngineResponse ans = new AskToolModelEngineResponse(list, 1, 1);

        assertNotNull(ans);
        assertEquals("[{\"name\":\"name\",\"arguments\":{\"key\":\"value\"},\"id\":\"id\"}]", ans.getStringResponse());
        assertEquals("id", ans.getToolCallId());
        assertEquals("name", ans.getToolCallName());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void emptyMap() {
        Map<String, Object> map = new HashMap();
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        AskToolModelEngineResponse ans = new AskToolModelEngineResponse(list, 0, 0);

        assertNotNull(ans);
        assertEquals("[{}]", ans.getStringResponse());
        assertNull(ans.getToolCallId());
        assertNull(ans.getToolCallName());
    }

//    @Test
//    void fromMap() {
//        Map<String, Object> map = new HashMap();
//        map.put("numberOfTokensInPrompt", 1);
//        map.put("numberOfTokensInResponse", 2);
//
//        AskToolModelEngineResponse ans = reactor.fromMap(map);
//
//        assertNotNull(ans);
//        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
//        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
//    }

    @Test
    void toolCallArgs() {
        Map<String, Object> map = new HashMap();
        map.put("arguments", ((Map) new HashMap()).toString());

        reactor = new AskToolModelEngineResponse(new ArrayList<Map<String, Object>>() {{add(map);}}, 0, 0);
        String str = reactor.getToolCallArgumentsAsString();

        assertNotNull(str);
        assertTrue(str.equals("{}"));
    }

    // TODO: Try and force a JsonProcessingException
    @Test
    void toolCallProcessingError() {
        Map<String, Object> map = new HashMap();
        map.put("arguments", ((Map) new HashMap()).toString());

        reactor = new AskToolModelEngineResponse(new ArrayList<Map<String, Object>>() {{add(map);}}, 0, 0);
        String str = reactor.getToolCallArgumentsAsString();

        assertNotNull(str);
        assertTrue(str.equals("{}"));
    }

    @Test
    void toolCallArgsNull() {
        Map<String, Object> map = new HashMap();

        reactor = new AskToolModelEngineResponse(new ArrayList<Map<String, Object>>() {{add(map);}}, 0, 0);
        String str = reactor.getToolCallArgumentsAsString();

        assertNotNull(str);
        assertTrue(str.equals("{}"));
    }
}
