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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;


public class EmbeddingsModelEngineResponseUnitTests {
    EmbeddingsModelEngineResponse reactor;

    @Test
    void test() {
        List<Double> list = new ArrayList(){{add(1.0);}};
        List<List<Double>> response = new ArrayList(list);

        EmbeddingsModelEngineResponse ans = new EmbeddingsModelEngineResponse(response, 1, 1);

        assertNotNull(ans);
        assertEquals("[1.0]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void resonseObjIsString() {
        Map<String, Object> map = new HashMap();
        map.put("response", "string");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            reactor.fromMap(map);
        });
        assertEquals("string", e.getMessage());
    }

    @Test
    void fromObject() {
        List<Double> list = new ArrayList(){{add(0.5);}};
        Map<String, Object> map = new HashMap();
        map.put("response", (List)(new ArrayList<>(list)));
        map.put("numberOfTokensInPrompt", 1);
        map.put("numberOfTokensInResponse", 2);

        EmbeddingsModelEngineResponse ans = reactor.fromObject(map);

        assertNotNull(ans);
        assertEquals("[0.5]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void nullResponse() {
        EmbeddingsModelEngineResponse ans = reactor.fromJson(null);

        assertNull(ans);
    }

    @Test
    void fromJsonJSONArray() throws Exception {
        List<Double> list = new ArrayList(){{add(1.0);}};
        JSONArray arr = new JSONArray(){{put("{\"embeddings\":[" + list + "]}");}};
        JSONObject obj = new JSONObject();
        obj.put("output", arr);
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 2);

        EmbeddingsModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("[[1.0]]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonJSONArrayError() throws Exception {
        List<Double> list = new ArrayList(){{add(1.0);}};
        JSONArray arr = new JSONArray(){{put("\"embeddings\":[" + list + "]}");}};
        JSONObject obj = new JSONObject();
        obj.put("output", arr);
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 2);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            reactor.fromJson(obj);
        });
        assertEquals("Failed to parse embeddings JSON: A JSONObject text must begin with '{' at 1 [character 2 line 1]", e.getMessage());
    }

    @Test
    void fromJsonJSONObject() throws Exception {
        List<Double> list = new ArrayList(){{add(1.0);}};
        JSONObject obj = new JSONObject();
        obj.put("output", new JSONObject(){{put("embeddings", new JSONArray(){{put(list);}});}});
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 2);

        EmbeddingsModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("[[1.0]]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonJSONString() throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("output", "{\"embeddings\":[[1.0]]}");
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 2);

        EmbeddingsModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("[[1.0]]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonJSONStringError() throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("output", "\"embeddings\":[[1.0]]}");
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 2);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            reactor.fromJson(obj);
        });
        assertEquals("Failed to parse embeddings JSON: A JSONObject text must begin with '{' at 1 [character 2 line 1]", e.getMessage());
    }

    @Test
    void fromJsonNoEmbeddings() throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("output", "{\"foo\":[[1.0]]}");
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 2);

        EmbeddingsModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("[]", ans.getResponse().toString());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(2, (int) ans.getNumberOfTokensInResponse());
    }
}
