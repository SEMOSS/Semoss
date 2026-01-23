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
// mvn -q -Dtest=GetAllStepsReactorUnitTests test

package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Execution(ExecutionMode.SAME_THREAD)
class GetAllStepsReactorUnitTests {

    private GetAllStepsReactor reactor;
    private Map<String, String> keyValues;
    private Insight insight;

    @BeforeEach
    void setUp() {
        reactor = new GetAllStepsReactor();
        keyValues = reactor.keyValue;
        insight = mock(Insight.class);
        reactor.setInsight(insight);
    }

    @Test
    void execute_appendsStepsAndAdvancesIds() {
        keyValues.put("project", "proj-1");
        keyValues.put("sessionId", "sess-1");
        keyValues.put("fileName", "recording.json");

        User user = mock(User.class);
        PlaywrightSession session = mock(PlaywrightSession.class);
        session.history = new StepsEnvelope("1", PlaywrightSession.newMeta(""), new HashMap<>());
        session.lastStepId = 5;
        when(insight.getUser()).thenReturn(user);
        when(user.getPlaywrightSession("sess-1")).thenReturn(session);

        PlaywrightStep click = new PlaywrightStep(1, PlaywrightStepType.CLICK, null, new Coords(1, 2), null, null,
                null, null, null, null, null, null, null, "click", "desc", false, false, null, null, Boolean.TRUE,
                Boolean.TRUE, null, null);
        Map<String, List<List<PlaywrightStep>>> steps = Map.of("tab-1", List.of(List.of(click)));
        StepsEnvelope envelope = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), steps);

        try (MockedStatic<PlaywrightUtility> util = Mockito.mockStatic(PlaywrightUtility.class)) {
            util.when(() -> PlaywrightUtility.loadStepsFromFile("proj-1", "recording.json")).thenReturn(envelope);

            NounMetadata result = reactor.execute();
            assertEquals(PixelDataType.MAP, result.getNounType());

            assertEquals(6, session.lastStepId);
            assertEquals(1, session.history.steps().get("tab-1").size());

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) result.getValue();
            assertEquals(Boolean.TRUE, payload.get("success"));
        }
    }

    @Test
    void execute_missingSessionIdThrows() {
        keyValues.put("project", "proj-1");
        keyValues.put("fileName", "recording.json");
        keyValues.put("sessionId", "");

        assertThrows(IllegalArgumentException.class, () -> reactor.execute());
    }
}
