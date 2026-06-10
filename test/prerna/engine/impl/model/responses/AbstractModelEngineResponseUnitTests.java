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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class AbstractModelEngineResponseUnitTests {
    private AbstractModelEngineResponse<String> abs;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        abs = mock(AbstractModelEngineResponse.class, Mockito.withSettings()
            .useConstructor("", 1, 1)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }

    @Test
    void test() {
        abs.setResponse("response");
        abs.setNumberOfTokensInPrompt(1);
        abs.setNumberOfTokensInResponse(1);
        abs.setUsageRestriction(new HashMap<>());

        assertEquals("response", abs.getResponse());
        assertEquals(1, (int) abs.getNumberOfTokensInPrompt());
        assertEquals(1, (int) abs.getNumberOfTokensInResponse());
        assertEquals(new HashMap<>(), abs.getUsageRestriction());
        assertEquals(0, (int) abs.getTokens(new Integer(0)));
        assertEquals(0, (int) abs.getTokens(new Long(0)));
        assertEquals(0, (int) abs.getTokens(new Double(0)));
        assertEquals(0, (int) abs.getTokens(new Float(0.0)));
        assertEquals(0, (int) abs.getTokens("0"));
        assertNull(abs.getTokens(null));
        
        Map<String, Object> map = new HashMap<>();
        map.put("response", "response");
        map.put("numberOfTokensInPrompt", 1);
        map.put("numberOfTokensInResponse", 1);
        map.put("usageRestriction", new HashMap<>());
        assertEquals(map.get("response"), abs.toMap().get("response"));
        assertEquals(map.get("numberOfTokensInPrompt"), abs.toMap().get("numberOfTokensInPrompt"));
        assertEquals(map.get("numberOfTokensInResponse"), abs.toMap().get("numberOfTokensInResponse"));
        assertEquals(map.get("usageRestriction"), abs.toMap().get("usageRestriction"));
    }
}
