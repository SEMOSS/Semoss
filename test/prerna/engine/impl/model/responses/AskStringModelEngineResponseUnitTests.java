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

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class AskStringModelEngineResponseUnitTests {
    private AskStringModelEngineResponse reactor;

    @Test
    void constructorTest() {
        AskStringModelEngineResponse ans = new AskStringModelEngineResponse("response", 1, 1);

        assertNotNull(ans);
        assertEquals("response", ans.getStringResponse());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromNull() {
        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(null);

        assertNull(ans);
    }

    @Test
    void fromJson() {
        JSONObject obj = new JSONObject();
        obj.put("output", new JSONObject());
        obj.put("input_tokens", 1);
        obj.put("output_tokens", 1);

        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("{}", ans.getResponse());
        assertEquals(1, (int) ans.getNumberOfTokensInPrompt());
        assertEquals(1, (int) ans.getNumberOfTokensInResponse());
    }

    @Test
    void fromJsonOutputString() {
        JSONObject obj = new JSONObject();
        obj.put("output", "output");

        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("output", ans.getResponse());
    }

    @Test
    void fromJsonNoOutput() {
        JSONObject obj = new JSONObject();

        reactor = new AskStringModelEngineResponse("", 0, 0);
        AskStringModelEngineResponse ans = reactor.fromJson(obj);

        assertNotNull(ans);
        assertEquals("", ans.getResponse());
    }
}
