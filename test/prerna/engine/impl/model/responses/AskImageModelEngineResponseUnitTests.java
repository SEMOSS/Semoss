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
