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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class AskImageModelEngineResponseUnitTests {
    private AskImageModelEngineResponse reactor;

    @Test
    void getKServeOutputElse() {
        Map<String, Object> map = new HashMap();
        map.put("string", "string");

        JSONObject jsonObj  = new JSONObject();
        jsonObj.put("output", map);
        jsonObj.put("prompt", "prompt");
        jsonObj.put("negative_prompt", "negative_prompt");
        jsonObj.put("height", 1);
        jsonObj.put("width", 1);
        jsonObj.put("num_inference_steps", 1);
        jsonObj.put("guidance_scale", 1.0);
        jsonObj.put("seed", 1);

        reactor = new AskImageModelEngineResponse(null, 0, 0);
        AskImageModelEngineResponse obj = reactor.getKServeImageResponse(jsonObj);

        assertNotNull(obj);

        assertNotNull(obj.getResponse().get("images"));
        assertEquals("{\"string\":\"string\"}", obj.getResponse().get("images").toString());
        assertEquals(0, (int) obj.getNumberOfTokensInPrompt());
        assertEquals(0,(int) obj.getNumberOfTokensInResponse());

        assertEquals("prompt", obj.getResponse().get("prompt"));
        assertEquals("negative_prompt", obj.getResponse().get("negative_prompt"));
        assertEquals(1, obj.getResponse().get("height"));
        assertEquals(1, obj.getResponse().get("width"));
        assertEquals(1, obj.getResponse().get("num_inference_steps"));
        assertEquals(1.0, obj.getResponse().get("guidance_scale"));
        assertEquals(1, obj.getResponse().get("seed"));
    }

    @Test
    void getKServeOutputJsonArray() {
        ArrayList list = new ArrayList();
        list.add("JsonArray");
        JSONObject jsonObj  = new JSONObject();
        jsonObj.put("output", list);

        reactor = new AskImageModelEngineResponse(null, 0, 0);
        AskImageModelEngineResponse obj = reactor.getKServeImageResponse(jsonObj);

        assertNotNull(obj);
        assertNotNull(obj.getResponse().get("images"));
        assertEquals("JsonArray", ((String[])obj.getResponse().get("images"))[0]);
        assertEquals(0, (int) obj.getNumberOfTokensInPrompt());
        assertEquals(0, (int) obj.getNumberOfTokensInResponse());
        
    }

    @Test
    void getKserverOutputStringJsonArray() {
        JSONObject jsonObj  = new JSONObject();
        jsonObj.put("output", "[string]");

        reactor = new AskImageModelEngineResponse(null, 0, 0);
        AskImageModelEngineResponse obj = reactor.getKServeImageResponse(jsonObj);

        assertNotNull(obj);
        assertNotNull(obj.getResponse().get("images"));
        assertEquals("string", ((String[])obj.getResponse().get("images"))[0]);
        assertEquals(0, (int) obj.getNumberOfTokensInPrompt());
        assertEquals(0, (int) obj.getNumberOfTokensInResponse());
        
    }

    @Test
    void getKserverOutputStringCatch() {
        JSONObject jsonObj  = new JSONObject();
        jsonObj.put("output", "[{json:obj}]");

        reactor = new AskImageModelEngineResponse(null, 0, 0);
        AskImageModelEngineResponse obj = reactor.getKServeImageResponse(jsonObj);

        assertNotNull(obj);
        assertNotNull(obj.getResponse().get("images"));
        assertEquals("[{json:obj}]", obj.getResponse().get("images").toString());
    }

    @Test
    void getKserverNullParam() {
        reactor = new AskImageModelEngineResponse(null, 0, 0);
        AskImageModelEngineResponse obj = reactor.getKServeImageResponse(null);
        
        assertNotNull(obj);
        assertEquals("error", obj.getResponse().get("status"));
        assertEquals("Null response from model request", obj.getResponse().get("message"));
        assertEquals(0, (int) obj.getNumberOfTokensInPrompt());
        assertEquals(0, (int) obj.getNumberOfTokensInResponse());
    }

    @Test
    void getStringImagesJsonArray() {
        Map<String, Object> map = new HashMap();
        // map.put("images", new ArrayList(){{add(1);}});
        map.put("images", new JSONArray());

        reactor = new AskImageModelEngineResponse(map, 1, 1);
        String str = reactor.getStringResponse();

        assertNotNull(str);
        assertEquals("{\"images\":[]}", str);
        assertEquals(1, (int) reactor.getNumberOfTokensInPrompt());
        assertEquals(1, (int) reactor.getNumberOfTokensInResponse());
    }

    @Test
    void getStringImagesIfElse() {
        Map<String, Object> map = new HashMap();
        map.put("images", "string");

        reactor = new AskImageModelEngineResponse(map, 0, 0);
        String str = reactor.getStringResponse();

        assertNotNull(str);
        assertEquals("{\"images\":\"string\"}", str);
    }

    @Test
    void getStringImagesElse() {
        Map<String, Object> map = new HashMap();
        map.put("key", "value");

        reactor = new AskImageModelEngineResponse(map, 0, 0);
        String str = reactor.getStringResponse();

        assertNotNull(str);
        assertEquals("{\"key\":\"value\"}", str);
    }
}
